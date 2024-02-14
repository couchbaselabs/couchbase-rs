use crate::memdx::codec::KeyValueCodec;
use crate::memdx::dispatcher::{DispatchFn, Dispatcher};
use crate::memdx::error::{CancellationErrorKind, Error};
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::{ClientPendingOp, PendingOp};
use futures::{SinkExt, StreamExt};
use log::{debug, trace, warn};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;
type OpaqueMap = HashMap<u32, Box<Arc<DispatchFn>>>;
pub(crate) type CancellationSender = Sender<(u32, CancellationErrorKind)>;

pub enum Connection {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

static HANDLER_INVOKE_PERMITS: Semaphore = Semaphore::const_new(1);

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

        let (cancel_tx, cancel_rx) = mpsc::channel();

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

    fn register_handler(&mut self, handler: Box<Arc<DispatchFn>>) -> u32 {
        let requests = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().unwrap();

        let opaque = self.current_opaque;
        self.current_opaque += 1;

        map.insert(opaque, handler);

        opaque
    }

    async fn cancel_loop(
        cancel_rx: Receiver<(u32, CancellationErrorKind)>,
        opaque_map: Arc<Mutex<OpaqueMap>>,
    ) {
        loop {
            match cancel_rx.recv() {
                Ok(cancel_info) => {
                    let permit = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
                    let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                    let mut map = requests.lock().unwrap();

                    let t = map.remove(&cancel_info.0);

                    if let Some(map_entry) = t {
                        let sender = Arc::clone(&map_entry);
                        drop(map);

                        sender(Err(Error::Cancelled(cancel_info.1)));
                    }

                    drop(permit);
                }
                Err(e) => {
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
                            packet.op_code(),
                            packet.opaque(),
                            packet.status(),
                        );

                        let opaque = packet.opaque();

                        let permit = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
                        let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                        let map = requests.lock().unwrap();

                        // We remove and then re-add if there are more packets so that we don't have
                        // to hold the opaque map mutex across the callback.
                        let t = map.get(&opaque);

                        if let Some(map_entry) = t {
                            let sender = Arc::clone(map_entry);
                            drop(map);
                            let has_more_packets = sender(Ok(packet));
                            drop(sender);

                            if !has_more_packets {
                                let mut map = requests.lock().unwrap();
                                map.remove(&opaque);
                                drop(map);
                            }
                        } else {
                            drop(map);
                            warn!(
                                "{} has no entry in request map for {}",
                                client_id,
                                &packet.opaque()
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
    async fn dispatch(
        &mut self,
        mut packet: RequestPacket,
        handler: impl (Fn(Result<ResponsePacket>) -> bool) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp> {
        let opaque = self.register_handler(Box::new(Arc::new(handler)));
        packet = packet.set_opaque(opaque);
        let op_code = packet.op_code();

        match self.writer.send(packet).await {
            Ok(_) => Ok(ClientPendingOp::new(opaque, self.cancel_tx.clone())),
            Err(e) => {
                debug!(
                    "{} failed to write packet {} {} {}",
                    self.client_id, opaque, op_code, e
                );

                let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
                let mut map = requests.lock().unwrap();
                map.remove(&opaque);

                Err(Error::Dispatch(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::memdx::client::{Client, Connection};
    use crate::memdx::dispatcher::Dispatcher;
    use crate::memdx::hello_feature::HelloFeature;
    use crate::memdx::magic::Magic;
    use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap};
    use crate::memdx::opcode::OpCode;
    use crate::memdx::ops_core::OpsCore;
    use crate::memdx::packet::{RequestPacket, ResponsePacket};
    use crate::memdx::request::HelloRequest;
    use std::sync::mpsc;
    use tokio::net::TcpStream;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let socket = TcpStream::connect("127.0.0.1:11210")
            .await
            .expect("could not connect");

        let conn = Connection::Tcp(socket);
        let mut client = Client::new(conn);

        let bootstrap_result = OpBootstrap::bootstrap(
            OpsCore {},
            &mut client,
            BootstrapOptions {
                hello: Some(HelloRequest {
                    client_name: "test-client".into(),
                    requested_features: vec![
                        HelloFeature::AltRequests,
                        HelloFeature::Collections,
                        HelloFeature::Duplex,
                    ],
                }),
            },
        )
        .await
        .unwrap();
        dbg!(&bootstrap_result);

        let hello_result = bootstrap_result.hello.unwrap();
        assert_eq!(3, hello_result.enabled_features.len());

        let (sender, recv) = mpsc::sync_channel::<ResponsePacket>(1);

        let req = RequestPacket::new(Magic::Req, OpCode::Set);
        match client
            .dispatch(req, move |resp| -> bool {
                sender
                    .send(resp.unwrap())
                    .expect("Failed to send on channel");
                true
            })
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("Failed to dispatch request {}", e),
        };

        let result = recv.recv().unwrap();

        dbg!(result);
    }
}
