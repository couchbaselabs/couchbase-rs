use std::collections::HashMap;
use std::env;
use std::io::empty;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::spawn;

use async_trait::async_trait;
use futures::SinkExt;
use log::{debug, trace, warn};
use tokio::io::{Join, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, Mutex, oneshot, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tokio_rustls::client::TlsStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::memdx::client_response::ClientResponse;
use crate::memdx::codec::KeyValueCodec;
use crate::memdx::connection::{Connection, ConnectionType};
use crate::memdx::dispatcher::{Dispatcher, DispatcherOptions};
use crate::memdx::error::{CancellationErrorKind, Error};
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

pub type Result<T> = std::result::Result<T, Error>;
type ResponseSender = Sender<Result<ClientResponse>>;
type OpaqueMap = HashMap<u32, Arc<ResponseSender>>;
pub(crate) type CancellationSender = UnboundedSender<(u32, CancellationErrorKind)>;

#[derive(Debug)]
struct ReadLoopOptions {
    pub client_id: String,
    pub local_addr: Option<SocketAddr>,
    pub peer_addr: Option<SocketAddr>,
    pub orphan_handler: UnboundedSender<ResponsePacket>,
    pub on_connection_close_tx: Option<oneshot::Sender<Result<()>>>,
    pub on_client_close_rx: Receiver<()>,
}

#[derive(Debug)]
pub struct Client {
    current_opaque: u32,
    opaque_map: Arc<Mutex<OpaqueMap>>,

    client_id: String,

    writer: FramedWrite<WriteHalf<TcpStream>, KeyValueCodec>,
    read_handle: JoinHandle<()>,

    cancel_tx: CancellationSender,
    close_tx: Sender<()>,

    local_addr: Option<SocketAddr>,
    peer_addr: Option<SocketAddr>,
}

impl Client {
    async fn register_handler(&mut self, handler: Arc<ResponseSender>) -> u32 {
        let requests = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().await;

        let opaque = self.current_opaque;
        self.current_opaque += 1;

        map.insert(opaque, handler);

        opaque
    }

    async fn read_loop(
        mut stream: FramedRead<ReadHalf<TcpStream>, KeyValueCodec>,
        mut op_cancel_rx: UnboundedReceiver<(u32, CancellationErrorKind)>,
        opaque_map: Arc<Mutex<OpaqueMap>>,
        mut opts: ReadLoopOptions,
    ) {
        loop {
            select! {
                (_) = opts.on_client_close_rx.recv() => {
                    drop(stream);
                    drop(op_cancel_rx);
                    if let Some(handler) = opts.on_connection_close_tx {
                        handler.send(Ok(())).unwrap();
                        return;
                    }
                    return;
                },
                (cancel_reason) = op_cancel_rx.recv() => {
                    match cancel_reason {
                        Some(cancel_info) => {
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
                        }
                        None => {
                            return;
                        }
                    }
                },
                (next) = stream.next() => {
                    match next {
                        Some(input) => {
                            match input {
                                Ok(packet) => {
                                    trace!(
                                        "Resolving response on {}. Opcode={}. Opaque={}. Status={}",
                                        opts.client_id,
                                        packet.op_code,
                                        packet.opaque,
                                        packet.status,
                                    );

                                    let opaque = packet.opaque;

                                    let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                                    let map = requests.lock().await;

                                    // We remove and then re-add if there are more packets so that we don't have
                                    // to hold the opaque map mutex across the callback.
                                    let t = map.get(&opaque);

                                    if let Some(map_entry) = t {
                                        let sender = Arc::clone(map_entry);
                                        drop(map);
                                        let (more_tx, more_rx) = oneshot::channel();

                                        // TODO: clone
                                        let resp = ClientResponse::new(packet, more_tx, opts.peer_addr, opts.local_addr);
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
                                        let opaque = packet.opaque;
                                        match opts.orphan_handler.send(packet) {
                                            Ok(_) => {}
                                            Err(_) => {
                                                warn!(
                                                    "{} failed to send packet to orphan handler {}",
                                                    opts.client_id, opaque
                                                );
                                            }
                                        };
                                    }
                                    drop(requests);
                                }
                                Err(e) => {
                                    warn!("{} failed to read frame {}", opts.client_id, e.to_string());
                                }
                            }
                        }
                        None => {
                            if let Some(handler) = opts.on_connection_close_tx {
                                handler.send(Ok(())).unwrap();
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Dispatcher for Client {
    fn new(conn: Connection, opts: DispatcherOptions) -> Self {
        let local_addr = *conn.local_addr();
        let peer_addr = *conn.peer_addr();

        let (r, w) = match conn.into_inner() {
            ConnectionType::Tcp(stream) => tokio::io::split(stream),
            ConnectionType::Tls(stream) => {
                let (tcp, _) = stream.into_inner();
                tokio::io::split(tcp)
            }
        };

        let codec = KeyValueCodec::default();
        let reader = FramedRead::new(r, codec);
        let writer = FramedWrite::new(w, codec);

        let uuid = Uuid::new_v4().to_string();

        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();
        let (close_tx, close_rx) = mpsc::channel::<()>(1);

        let opaque_map = Arc::new(Mutex::new(OpaqueMap::default()));

        let read_opaque_map = Arc::clone(&opaque_map);
        let read_uuid = uuid.clone();

        let read_handle = tokio::spawn(async move {
            Client::read_loop(
                reader,
                cancel_rx,
                read_opaque_map,
                ReadLoopOptions {
                    client_id: read_uuid,
                    local_addr,
                    peer_addr,
                    orphan_handler: opts.orphan_handler,
                    on_connection_close_tx: opts.on_connection_close_handler,
                    on_client_close_rx: close_rx,
                },
            )
            .await;
        });

        Self {
            current_opaque: 1,
            opaque_map,
            client_id: uuid,

            cancel_tx,
            close_tx,

            writer,
            read_handle,

            local_addr,
            peer_addr,
        }
    }

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

    async fn close(mut self) -> Result<()> {
        let mut close_err = None;
        match self.writer.close().await {
            Ok(_) => {}
            Err(e) => {
                close_err = Some(e);
            }
        };
        self.close_tx.send(()).await.unwrap_or_default();
        self.read_handle.await.unwrap_or_default();

        let map = self.opaque_map.lock().await;
        for entry in map.iter() {
            entry
                .1
                .send(Err(Error::ClosedInFlight))
                .await
                .unwrap_or_default();
        }

        if let Some(e) = close_err {
            return Err(Error::from(e));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;
    use tokio::time::Instant;

    use crate::memdx::auth_mechanism::AuthMechanism::{ScramSha1, ScramSha256, ScramSha512};
    use crate::memdx::client::{Client, Connection};
    use crate::memdx::connection::ConnectOptions;
    use crate::memdx::dispatcher::{Dispatcher, DispatcherOptions};
    use crate::memdx::hello_feature::HelloFeature;
    use crate::memdx::op_auth_saslauto::SASLAuthAutoOptions;
    use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap};
    use crate::memdx::ops_core::OpsCore;
    use crate::memdx::ops_crud::OpsCrud;
    use crate::memdx::packet::ResponsePacket;
    use crate::memdx::request::{
        GetClusterConfigRequest, GetErrorMapRequest, GetRequest, HelloRequest, SelectBucketRequest,
        SetRequest,
    };
    use crate::memdx::response::{GetResponse, SetResponse};
    use crate::memdx::sync_helpers::sync_unary_call;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let conn = Connection::connect(
            "192.168.107.128:11210".parse().unwrap(),
            ConnectOptions {
                tls_config: None,
                deadline: instant,
            },
        )
        .await
        .expect("Could not connect");

        let (orphan_tx, mut orphan_rx) = unbounded_channel::<ResponsePacket>();
        let (close_tx, mut close_rx) = oneshot::channel::<crate::memdx::client::Result<()>>();

        tokio::spawn(async move {
            loop {
                match orphan_rx.recv().await {
                    Some(resp) => {
                        dbg!("unexpected orphan", resp);
                    }
                    None => {
                        return;
                    }
                }
            }
        });

        tokio::spawn(async move {
            loop {
                if let Ok(resp) = close_rx.try_recv() {
                    dbg!("closed");
                    return;
                }
            }
        });

        let mut client = Client::new(
            conn,
            DispatcherOptions {
                on_connection_close_handler: Some(close_tx),
                orphan_handler: orphan_tx,
            },
        );

        let username = "Administrator".to_string();
        let password = "password".to_string();

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
                get_cluster_config: Some(GetClusterConfigRequest {}),
            },
        )
        .await
        .unwrap();
        dbg!(&bootstrap_result.hello);

        let hello_result = bootstrap_result.hello.unwrap();

        dbg!(
            std::str::from_utf8(bootstrap_result.cluster_config.unwrap().config.as_slice())
                .unwrap()
        );

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

        client.close().await.unwrap();
    }
}
