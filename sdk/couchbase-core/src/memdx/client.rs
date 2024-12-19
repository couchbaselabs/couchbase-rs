use std::cell::RefCell;
use std::collections::HashMap;
use std::io::empty;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::spawn;
use std::{env, mem};

use crate::log::{LogContext, LogContextAware};
use crate::memdx::client_response::ClientResponse;
use crate::memdx::codec::KeyValueCodec;
use crate::memdx::connection::{ConnectionType, Stream};
use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::dispatcher::{
    Dispatcher, DispatcherOptions, OnConnectionCloseHandler, OrphanResponseHandler,
};
use crate::memdx::error;
use crate::memdx::error::{CancellationErrorKind, Error, ErrorKind};
use crate::memdx::hello_feature::HelloFeature::DataType;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::subdoc::SubdocRequestInfo;
use async_trait::async_trait;
use futures::{SinkExt, TryFutureExt};
use log::{debug, error, log, trace, warn};
use snap::raw::Decoder;
use tokio::io::{AsyncRead, AsyncWrite, Join, ReadHalf, WriteHalf};
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot, Mutex, MutexGuard, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub(crate) type ResponseSender = Sender<error::Result<ClientResponse>>;
pub(crate) type OpaqueMap = HashMap<u32, Arc<SenderContext>>;

#[derive(Debug, Clone, Copy)]
pub struct ResponseContext {
    pub cas: Option<u64>,
    pub subdoc_info: Option<SubdocRequestInfo>,
}

#[derive(Debug, Clone)]
pub(crate) struct SenderContext {
    pub sender: ResponseSender,
    pub context: ResponseContext,
}

struct ReadLoopOptions {
    pub logger_context: LogContext,
    pub orphan_handler: OrphanResponseHandler,
    pub on_connection_close_tx: OnConnectionCloseHandler,
    pub on_client_close_rx: Receiver<()>,
    pub disable_decompression: bool,
}

#[derive(Debug)]
struct ClientReadHandle {
    read_handle: JoinHandle<()>,
}

impl ClientReadHandle {
    pub async fn await_completion(&mut self) {
        (&mut self.read_handle).await.unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct Client {
    current_opaque: AtomicU32,
    opaque_map: Arc<std::sync::Mutex<OpaqueMap>>,

    writer: Mutex<FramedWrite<WriteHalf<Box<dyn Stream>>, KeyValueCodec>>,
    read_handle: Mutex<ClientReadHandle>,
    close_tx: Sender<()>,

    local_addr: SocketAddr,
    peer_addr: SocketAddr,

    closed: AtomicBool,

    log_context: LogContext,
}

impl Client {
    fn register_handler(&self, response_context: SenderContext) -> u32 {
        let mut map = self.opaque_map.lock().unwrap();

        let opaque = self.current_opaque.fetch_add(1, Ordering::SeqCst);

        map.insert(opaque, Arc::new(response_context));

        opaque
    }

    async fn drain_opaque_map(opaque_map: Arc<std::sync::Mutex<OpaqueMap>>) {
        let mut senders = vec![];
        {
            let mut guard = opaque_map.lock().unwrap();
            guard.drain().for_each(|(_, v)| {
                senders.push(v);
            });
        }

        for sender in senders {
            sender
                .sender
                .send(Err(ErrorKind::Cancelled(
                    CancellationErrorKind::ClosedInFlight,
                )
                .into()))
                .await
                .unwrap_or_default();
        }
    }

    async fn on_read_loop_close(
        stream: FramedRead<ReadHalf<Box<dyn Stream>>, KeyValueCodec>,
        opaque_map: Arc<std::sync::Mutex<OpaqueMap>>,
        on_connection_close: OnConnectionCloseHandler,
    ) {
        drop(stream);

        Self::drain_opaque_map(opaque_map).await;

        on_connection_close().await;
    }

    async fn read_loop(
        mut stream: FramedRead<ReadHalf<Box<dyn Stream>>, KeyValueCodec>,
        opaque_map: Arc<std::sync::Mutex<OpaqueMap>>,
        mut opts: ReadLoopOptions,
    ) {
        loop {
            select! {
                (_) = opts.on_client_close_rx.recv() => {
                    Self::on_read_loop_close(stream, opaque_map, opts.on_connection_close_tx).await;
                    return;
                },
                (next) = stream.next() => {
                    match next {
                        Some(input) => {
                            match input {
                                Ok(mut packet) => {
                                    trace!(
                                        context=&opts.logger_context,
                                        opcode=packet.op_code,
                                        opaque=&packet.opaque,
                                        status=packet.status;
                                        "Resolving response",
                                    );

                                    let opaque = packet.opaque;

                                    let requests: Arc<std::sync::Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                                    let context = {
                                        let map = requests.lock().unwrap();

                                        // We remove and then re-add if there are more packets so that we don't have
                                        // to hold the opaque map mutex across the callback.
                                        let t = map.get(&opaque);

                                        t.map(Arc::clone)
                                    };


                                    if let Some(context) = context {
                                        let sender = &context.sender;
                                        let (more_tx, more_rx) = oneshot::channel();

                                        if let Some(value) = &packet.value {
                                            if !opts.disable_decompression && (packet.datatype & u8::from(DataTypeFlag::Compressed) != 0) {
                                                let mut decoder = Decoder::new();
                                                let new_value = match decoder
                                                    .decompress_vec(value)
                                                     {
                                                        Ok(v) => v,
                                                        Err(e) => {
                                                            match sender.send(Err(ErrorKind::Json {msg: e.to_string()}.into())).await{
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    debug!(context = &opts.logger_context, e:err; "Sending response to caller failed");
                                                                }
                                                            };
                                                         continue;
                                                        }
                                                    };

                                                packet.datatype &= !u8::from(DataTypeFlag::Compressed);
                                                packet.value = Some(new_value);
                                            }
                                        }

                                        // TODO: clone
                                        let resp = ClientResponse::new(packet, more_tx, context.context);
                                        match sender.send(Ok(resp)).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                debug!(context=&opts.logger_context, e:err; "Sending response to caller failed");
                                            }
                                        };
                                        drop(context);

                                        match more_rx.await {
                                            Ok(has_more_packets) => {
                                                if !has_more_packets {
                                                    let mut map = requests.lock().unwrap();
                                                    map.remove(&opaque);
                                                    drop(map);
                                                }
                                            }
                                            Err(_) => {
                                                // If the response gets dropped then the receiver will be closed,
                                                // which we treat as an implicit !has_more_packets.
                                                let mut map = requests.lock().unwrap();
                                                map.remove(&opaque);
                                                drop(map);
                                            }
                                        }
                                    } else {
                                        let opaque = packet.opaque;
                                        (opts.orphan_handler)(packet);
                                    }
                                    drop(requests);
                                }
                                Err(e) => {
                                    warn!(context=&opts.logger_context, e:err; "Failed to read");
                                }
                            }
                        }
                        None => {
                            Self::on_read_loop_close(stream, opaque_map, opts.on_connection_close_tx).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    fn split_stream<StreamType: AsyncRead + AsyncWrite + Send + Unpin>(
        stream: StreamType,
    ) -> (ReadHalf<StreamType>, WriteHalf<StreamType>) {
        tokio::io::split(stream)
    }
}

impl LogContextAware for Client {
    fn log_context(&self) -> &LogContext {
        &self.log_context
    }
}

#[async_trait]
impl Dispatcher for Client {
    fn new(conn: ConnectionType, opts: DispatcherOptions) -> Self {
        let local_addr = *conn.local_addr();
        let peer_addr = *conn.peer_addr();

        let (r, w) = tokio::io::split(conn.into_inner());

        let codec = KeyValueCodec::default();
        let reader = FramedRead::new(r, codec);
        let writer = FramedWrite::new(w, codec);

        let (close_tx, close_rx) = mpsc::channel::<()>(1);

        let opaque_map = Arc::new(std::sync::Mutex::new(OpaqueMap::default()));

        let read_opaque_map = Arc::clone(&opaque_map);

        let logger_context_clone = opts.log_context.clone();

        let read_handle = tokio::spawn(async move {
            Client::read_loop(
                reader,
                read_opaque_map,
                ReadLoopOptions {
                    orphan_handler: opts.orphan_handler,
                    on_connection_close_tx: opts.on_connection_close_handler,
                    on_client_close_rx: close_rx,
                    disable_decompression: opts.disable_decompression,
                    logger_context: logger_context_clone,
                },
            )
            .await;
        });

        Self {
            current_opaque: AtomicU32::new(1),
            opaque_map,

            close_tx,

            writer: Mutex::new(writer),
            read_handle: Mutex::new(ClientReadHandle { read_handle }),

            local_addr,
            peer_addr,

            closed: AtomicBool::new(false),

            log_context: opts.log_context,
        }
    }

    async fn dispatch(
        &self,
        mut packet: RequestPacket,
        response_context: Option<ResponseContext>,
    ) -> error::Result<ClientPendingOp> {
        let (response_tx, response_rx) = mpsc::channel(1);
        let opaque = self.register_handler(SenderContext {
            sender: response_tx,
            context: response_context.unwrap_or(ResponseContext {
                cas: packet.cas,
                subdoc_info: None,
            }),
        });
        packet.opaque = Some(opaque);
        let op_code = packet.op_code;

        trace!(
            context=&self.log_context,
            opcode=packet.op_code,
            opaque=opaque;
            "Writing request",
        );

        let mut writer = self.writer.lock().await;
        match writer.send(packet).await {
            Ok(_) => Ok(ClientPendingOp::new(
                opaque,
                self.opaque_map.clone(),
                response_rx,
            )),
            Err(e) => {
                debug!(
                    context=&self.log_context, e:err; "Failed to write packet {} {}",
                    opaque, op_code,
                );

                let requests: Arc<std::sync::Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
                {
                    let mut map = requests.lock().unwrap();
                    map.remove(&opaque);
                }

                Err(Error::dispatch_error(opaque, op_code, Box::new(e)))
            }
        }
    }

    async fn close(&self) -> error::Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        debug!(context=&self.log_context; "Closing");

        let mut close_err = None;
        let mut writer = self.writer.lock().await;
        match writer.close().await {
            Ok(_) => {}
            Err(e) => {
                close_err = Some(e);
            }
        };

        // TODO: We probably need to be logging any errors here.
        self.close_tx.send(()).await.unwrap_or_default();

        // Note: doing this doesn't technically consume the handle but calling it twice will
        // cause a panic.
        let mut read_handle = self.read_handle.lock().await;
        read_handle.await_completion().await;

        Self::drain_opaque_map(self.opaque_map.clone()).await;

        if let Some(e) = close_err {
            return Err(Error::close_error(e.to_string(), Box::new(e)));
        }

        Ok(())
    }
}
