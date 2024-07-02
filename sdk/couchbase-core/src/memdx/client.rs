use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use log::{debug, trace, warn};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, oneshot, Semaphore};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::memdx::codec::KeyValueCodec;
use crate::memdx::connection::Connection;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::{CancellationErrorKind, Error};
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

pub type Result<T> = std::result::Result<T, Error>;
pub type ResponseSender = Sender<Result<ClientResponse>>;
type OpaqueMap = HashMap<u32, Arc<ResponseSender>>;
pub(crate) type CancellationSender = UnboundedSender<(u32, CancellationErrorKind)>;

#[derive(Debug)]
pub(crate) struct ClientResponse {
    packet: ResponsePacket,
    has_more_sender: oneshot::Sender<bool>,
}

impl ClientResponse {
    pub fn new(packet: ResponsePacket, has_more_sender: oneshot::Sender<bool>) -> Self {
        Self {
            packet,
            has_more_sender,
        }
    }

    pub fn packet(&self) -> &ResponsePacket {
        &self.packet
    }

    pub fn send_has_more(self) {
        match self.has_more_sender.send(true) {
            Ok(_) => {}
            Err(_e) => {}
        };
    }
}

static HANDLER_INVOKE_PERMITS: Semaphore = Semaphore::const_new(1);

#[derive(Debug)]
pub struct Client {
    current_opaque: u32,
    opaque_map: Arc<Mutex<OpaqueMap>>,

    client_id: String,

    writer: FramedWrite<WriteHalf<TcpStream>, KeyValueCodec>,

    cancel_tx: CancellationSender,
}

impl Client {
    pub fn new(conn: Connection) -> Self {
        let (r, w) = match conn {
            Connection::Tcp(stream) => tokio::io::split(stream),
            Connection::Tls(stream) => {
                let (tcp, _) = stream.into_inner();
                tokio::io::split(tcp)
            }
        };

        let codec = KeyValueCodec::default();
        let reader = FramedRead::new(r, codec);
        let writer = FramedWrite::new(w, codec);

        let uuid = Uuid::new_v4().to_string();

        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();

        let client = Self {
            current_opaque: 1,
            opaque_map: Arc::new(Mutex::new(OpaqueMap::default())),
            client_id: uuid.clone(),

            cancel_tx,

            writer,
        };

        let read_opaque_map = Arc::clone(&client.opaque_map);
        tokio::spawn(async move {
            Client::read_loop(reader, read_opaque_map, uuid).await;
        });

        let cancel_opaque_map = Arc::clone(&client.opaque_map);
        tokio::spawn(async move {
            Client::cancel_loop(cancel_rx, cancel_opaque_map).await;
        });

        client
    }

    async fn register_handler(&mut self, handler: Arc<ResponseSender>) -> u32 {
        let requests = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().await;

        let opaque = self.current_opaque;
        self.current_opaque += 1;

        map.insert(opaque, handler);

        opaque
    }

    async fn cancel_loop(
        mut cancel_rx: UnboundedReceiver<(u32, CancellationErrorKind)>,
        opaque_map: Arc<Mutex<OpaqueMap>>,
    ) {
        loop {
            match cancel_rx.recv().await {
                Some(cancel_info) => {
                    let permit = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
                    let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                    let mut map = requests.lock().await;

                    let t = map.remove(&cancel_info.0);

                    if let Some(map_entry) = t {
                        let sender = Arc::clone(&map_entry);
                        drop(map);

                        sender
                            .send(Err(Error::Cancelled(cancel_info.1)))
                            .await
                            .unwrap();
                    } else {
                        drop(map);
                    }

                    drop(requests);
                    drop(permit);
                }
                None => {
                    return;
                }
            }
        }
    }

    async fn read_loop(
        mut stream: FramedRead<ReadHalf<TcpStream>, KeyValueCodec>,
        opaque_map: Arc<Mutex<OpaqueMap>>,
        client_id: String,
    ) {
        loop {
            if let Some(input) = stream.next().await {
                match input {
                    Ok(packet) => {
                        trace!(
                            "Resolving response on {}. Opcode={}. Opaque={}. Status={}",
                            client_id,
                            packet.op_code,
                            packet.opaque,
                            packet.status,
                        );

                        let opaque = packet.opaque;

                        let permit = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
                        let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                        let map = requests.lock().await;

                        // We remove and then re-add if there are more packets so that we don't have
                        // to hold the opaque map mutex across the callback.
                        let t = map.get(&opaque);

                        if let Some(map_entry) = t {
                            let sender = Arc::clone(map_entry);
                            drop(map);
                            let (more_tx, more_rx) = oneshot::channel();
                            let resp = ClientResponse::new(packet, more_tx);
                            match sender.send(Ok(resp)).await {
                                Ok(_) => {}
                                Err(e) => {
                                    debug!("Sending response to caller failed: {}", e);
                                }
                            };
                            drop(sender);

                            match more_rx.await {
                                Ok(has_more_packets) => {
                                    if !has_more_packets {
                                        let mut map = requests.lock().await;
                                        map.remove(&opaque);
                                        drop(map);
                                    }
                                }
                                Err(_) => {
                                    // If the response gets dropped then the receiver will be closed,
                                    // which we treat as an implicit !has_more_packets.
                                    let mut map = requests.lock().await;
                                    map.remove(&opaque);
                                    drop(map);
                                }
                            }
                        } else {
                            drop(map);
                            warn!(
                                "{} has no entry in request map for {}",
                                client_id, &packet.opaque
                            );
                        }
                        drop(requests);
                        drop(permit);
                    }
                    Err(e) => {
                        warn!("{} failed to read frame {}", client_id, e.to_string());
                    }
                }
            }
        }
    }
}

impl Dispatcher for Client {
    async fn dispatch(&mut self, mut packet: RequestPacket) -> Result<ClientPendingOp> {
        let (response_tx, response_rx) = mpsc::channel(1);
        let opaque = self.register_handler(Arc::new(response_tx)).await;
        packet.opaque = Some(opaque);
        let op_code = packet.op_code;

        match self.writer.send(packet).await {
            Ok(_) => Ok(ClientPendingOp::new(
                opaque,
                self.cancel_tx.clone(),
                response_rx,
            )),
            Err(e) => {
                debug!(
                    "{} failed to write packet {} {} {}",
                    self.client_id, opaque, op_code, e
                );

                let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
                let mut map = requests.lock().await;
                map.remove(&opaque);

                Err(Error::Dispatch(e.kind()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::memdx::auth_mechanism::AuthMechanism::{ScramSha1, ScramSha256, ScramSha512};
    use crate::memdx::client::{Client, Connection};
    use crate::memdx::connection::ConnectOptions;
    use crate::memdx::hello_feature::HelloFeature;
    use crate::memdx::op_auth_saslauto::SASLAuthAutoOptions;
    use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap};
    use crate::memdx::ops_core::OpsCore;
    use crate::memdx::ops_crud::OpsCrud;
    use crate::memdx::request::{
        GetErrorMapRequest, GetRequest, HelloRequest, SelectBucketRequest, SetRequest,
    };
    use crate::memdx::response::{GetResponse, SetResponse};
    use crate::memdx::sync_helpers::sync_unary_call;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let _ = env_logger::try_init();

        let conn = Connection::connect("192.168.107.128", 11210, ConnectOptions::default())
            .await
            .expect("Could not connect");

        let mut client = Client::new(conn);

        let username = "Administrator".to_string();
        let password = "password".to_string();

        let instant = Instant::now().add(Duration::new(7, 0));

        let bootstrap_result = OpBootstrap::bootstrap(
            OpsCore {},
            &mut client,
            BootstrapOptions {
                hello: Some(HelloRequest {
                    client_name: "test-client".into(),
                    requested_features: vec![
                        HelloFeature::DataType,
                        HelloFeature::SeqNo,
                        HelloFeature::Xattr,
                        HelloFeature::Xerror,
                        HelloFeature::Snappy,
                        HelloFeature::Json,
                        HelloFeature::UnorderedExec,
                        HelloFeature::Durations,
                        HelloFeature::SyncReplication,
                        HelloFeature::ReplaceBodyWithXattr,
                        HelloFeature::SelectBucket,
                        HelloFeature::CreateAsDeleted,
                        HelloFeature::AltRequests,
                        HelloFeature::Collections,
                        HelloFeature::Opentracing,
                    ],
                }),
                get_error_map: Some(GetErrorMapRequest { version: 2 }),
                auth: Some(SASLAuthAutoOptions {
                    username,
                    password,
                    enabled_mechs: vec![ScramSha512, ScramSha256, ScramSha1],
                }),
                select_bucket: Some(SelectBucketRequest {
                    bucket_name: "default".into(),
                }),
                deadline: instant,
            },
        )
        .await
        .unwrap();
        dbg!(&bootstrap_result.hello);

        let hello_result = bootstrap_result.hello.unwrap();
        assert_eq!(14, hello_result.enabled_features.len());

        let result: SetResponse = sync_unary_call(
            OpsCrud {
                collections_enabled: true,
                durability_enabled: true,
                preserve_expiry_enabled: false,
                ext_frames_enabled: true,
            }
            .set(
                &mut client,
                SetRequest {
                    collection_id: 0,
                    key: "test".as_bytes().into(),
                    vbucket_id: 1,
                    flags: 0,
                    value: "test".as_bytes().into(),
                    datatype: 0,
                    expiry: None,
                    preserve_expiry: None,
                    cas: None,
                    on_behalf_of: None,
                    durability_level: None,
                    durability_level_timeout: None,
                },
            ),
        )
        .await
        .unwrap();

        dbg!(result);

        let get_result: GetResponse = sync_unary_call(
            OpsCrud {
                collections_enabled: true,
                durability_enabled: true,
                preserve_expiry_enabled: false,
                ext_frames_enabled: true,
            }
            .get(
                &mut client,
                GetRequest {
                    collection_id: 0,
                    key: "test".as_bytes().into(),
                    vbucket_id: 1,
                    on_behalf_of: None,
                },
            ),
        )
        .await
        .unwrap();

        dbg!(get_result);
    }
}
