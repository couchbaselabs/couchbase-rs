use crate::memdx::codec::KeyValueCodec;
use crate::memdx::error::CancelledError;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::{ClientPendingOp, PendingOp};
use futures::{SinkExt, StreamExt};
use log::{debug, trace, warn};
use std::collections::HashMap;
use std::io;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

type DispatchFn = dyn (Fn(Result<ResponsePacket, CancelledError>) -> bool) + Send + Sync;
type OpaqueMap = HashMap<u32, Box<Arc<DispatchFn>>>;

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

    cancel_tx: Sender<u32>,
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

    pub async fn dispatch(
        &mut self,
        mut packet: RequestPacket,
        handler: impl (Fn(Result<ResponsePacket, CancelledError>) -> bool) + Send + Sync + 'static,
    ) -> Result<impl PendingOp, io::Error> {
        let _unused = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
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

                let requests = Arc::clone(&self.opaque_map);
                let mut map = requests.lock().unwrap();
                map.remove(&opaque);

                Err(e)
            }
        }
    }

    fn register_handler(&mut self, handler: Box<Arc<DispatchFn>>) -> u32 {
        let requests = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().unwrap();

        let opaque = self.current_opaque;
        self.current_opaque += 1;

        map.insert(opaque, handler);

        opaque
    }

    async fn cancel_loop(cancel_rx: Receiver<u32>, opaque_map: Arc<Mutex<OpaqueMap>>) {
        loop {
            match cancel_rx.recv() {
                Ok(opaque) => {
                    let permit = HANDLER_INVOKE_PERMITS.acquire().await.unwrap();
                    let requests = Arc::clone(&opaque_map);
                    let mut map = requests.lock().unwrap();

                    let t = map.remove(&opaque);

                    if let Some(map_entry) = t {
                        let sender = Arc::clone(&map_entry);
                        drop(map);

                        sender(Err(CancelledError {}));
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
                        let requests = Arc::clone(&opaque_map);
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

#[cfg(test)]
mod tests {
    use crate::memdx::client::{Client, Connection};
    use crate::memdx::magic::Magic;
    use crate::memdx::opcode::OpCode;
    use crate::memdx::packet::{RequestPacket, ResponsePacket};
    use std::sync::mpsc;
    use tokio::net::TcpStream;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let socket = TcpStream::connect("127.0.0.1:11210")
            .await
            .expect("could not connect");

        let conn = Connection::Tcp(socket);
        let mut client = Client::new(conn);

        let (sender, recv) = mpsc::sync_channel::<ResponsePacket>(1);

        let req = RequestPacket::new(Magic::Req, OpCode::Set, 0x01);
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
