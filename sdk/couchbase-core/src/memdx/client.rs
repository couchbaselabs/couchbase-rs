/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, TryFutureExt};
use log::{debug, error, trace, warn};
use snap::raw::Decoder;
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::empty;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::spawn;
use std::{env, mem};
use tokio::io::{AsyncRead, AsyncWrite, Join, ReadHalf, WriteHalf};
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot, Mutex, MutexGuard, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::{CancellationToken, DropGuard};
use uuid::Uuid;

use crate::memdx::client_response::ClientResponse;
use crate::memdx::codec::KeyValueCodec;
use crate::memdx::connection::{ConnectionType, Stream};
use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::dispatcher::{
    Dispatcher, DispatcherOptions, OnReadLoopCloseHandler, OrphanResponseHandler,
    UnsolicitedPacketHandler,
};
use crate::memdx::error;
use crate::memdx::error::{CancellationErrorKind, Error};
use crate::memdx::hello_feature::HelloFeature::DataType;
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::subdoc::SubdocRequestInfo;
use crate::orphan_reporter::OrphanContext;

pub(crate) type ResponseSender = Sender<error::Result<ClientResponse>>;
pub(crate) type OpaqueMap = HashMap<u32, SenderContext>;

#[derive(Debug, Clone)]
pub struct ResponseContext {
    pub cas: Option<u64>,
    pub subdoc_info: Option<SubdocRequestInfo>,
    pub scope_name: Option<String>,
    pub collection_name: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SenderContext {
    pub sender: ResponseSender,
    pub is_persistent: bool,
    pub context: Option<ResponseContext>,
}

struct ReadLoopOptions {
    pub client_id: String,
    pub unsolicited_packet_handler: UnsolicitedPacketHandler,
    pub orphan_handler: Option<OrphanResponseHandler>,
    pub on_read_close_handler: OnReadLoopCloseHandler,
    pub on_close_cancel: CancellationToken,
    pub disable_decompression: bool,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
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

    client_id: String,

    writer: Mutex<FramedWrite<WriteHalf<Box<dyn Stream>>, KeyValueCodec>>,
    on_close_cancel: DropGuard,

    local_addr: SocketAddr,
    peer_addr: SocketAddr,

    closed: AtomicBool,
}

impl Client {
    fn register_handler(&self, response_context: SenderContext) -> u32 {
        let mut map = self.opaque_map.lock().unwrap();

        let opaque = self.current_opaque.fetch_add(1, Ordering::SeqCst);

        map.insert(opaque, response_context);

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
                .send(Err(Error::new_cancelled_error(
                    CancellationErrorKind::ClosedInFlight,
                )))
                .await
                .unwrap_or_default();
        }
    }

    async fn on_read_loop_close(
        client_id: &str,
        stream: FramedRead<ReadHalf<Box<dyn Stream>>, KeyValueCodec>,
        opaque_map: Arc<std::sync::Mutex<OpaqueMap>>,
        on_read_loop_close: OnReadLoopCloseHandler,
    ) {
        drop(stream);

        Self::drain_opaque_map(opaque_map).await;

        on_read_loop_close().await;

        debug!("{client_id} read loop shut down");
    }

    async fn read_loop(
        mut stream: FramedRead<ReadHalf<Box<dyn Stream>>, KeyValueCodec>,
        opaque_map: Arc<std::sync::Mutex<OpaqueMap>>,
        mut opts: ReadLoopOptions,
    ) {
        loop {
            select! {
                (_) = opts.on_close_cancel.cancelled() => {
                    Self::on_read_loop_close(&opts.client_id, stream, opaque_map, opts.on_read_close_handler).await;
                    return;
                },
                (next) = stream.next() => {
                    match next {
                        Some(input) => {
                            match input {
                                Ok(mut packet) => {
                                    if packet.magic == Magic::ServerReq {

                                        trace!(
                                            "Handling server request on {}. Opcode={}",
                                            opts.client_id,
                                            packet.op_code,
                                        );

                                        (opts.unsolicited_packet_handler)(packet).await;
                                        continue;
                                    }

                                    trace!(
                                        "Resolving response on {}. Opcode={}. Opaque={}. Status={}",
                                        opts.client_id,
                                        packet.op_code,
                                        packet.opaque,
                                        packet.status,
                                    );

                                    let opaque = packet.opaque;

                                    let requests: Arc<std::sync::Mutex<OpaqueMap>> = Arc::clone(&opaque_map);
                                    let context = {
                                        let mut map = requests.lock().unwrap();
                                        map.remove(&opaque)
                                    };

                                    if let Some(mut context) = context {
                                        let sender = &context.sender;

                                        if let Some(value) = &packet.value {
                                            if !opts.disable_decompression && (packet.datatype & u8::from(DataTypeFlag::Compressed) != 0) {
                                                let mut decoder = Decoder::new();
                                                let new_value = match decoder
                                                    .decompress_vec(value)
                                                     {
                                                        Ok(v) => v,
                                                        Err(e) => {
                                                            match sender.send(Err(Error::new_decompression_error().with(e))).await{
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                     debug!("Sending response to caller failed: {e}");
                                                                }
                                                            };
                                                         continue;
                                                        }
                                                    };

                                                packet.datatype &= !u8::from(DataTypeFlag::Compressed);
                                                packet.value = Some(new_value);
                                            }
                                        }

                                        if context.is_persistent {
                                            {
                                                let mut map = requests.lock().unwrap();
                                                map.insert(opaque, context.clone());
                                            }
                                        }

                                        let resp = ClientResponse::new(packet, context.context);
                                        match sender.send(Ok(resp)).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                debug!("Sending response to caller failed: {e}");
                                                Self::on_read_loop_close(&opts.client_id, stream, opaque_map, opts.on_read_close_handler).await;
                                                return;
                                            }
                                        };
                                    } else if let Some(ref orphan_handler) = opts.orphan_handler {
                                        orphan_handler(
                                            packet,
                                            OrphanContext {
                                                client_id: opts.client_id.clone(),
                                                local_addr: opts.local_addr,
                                                peer_addr: opts.peer_addr,
                                            },
                                        );
                                    }
                                    drop(requests);
                                }
                                Err(e) => {
                                    warn!("{} failed to read frame {}", opts.client_id, e);
                                    Self::on_read_loop_close(&opts.client_id, stream, opaque_map, opts.on_read_close_handler).await;
                                    return;
                                }
                            }
                        }
                        None => {
                            Self::on_read_loop_close(&opts.client_id, stream, opaque_map, opts.on_read_close_handler).await;
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

#[async_trait]
impl Dispatcher for Client {
    fn new(conn: ConnectionType, opts: DispatcherOptions) -> Self {
        let local_addr = *conn.local_addr();
        let peer_addr = *conn.peer_addr();

        let (r, w) = tokio::io::split(conn.into_inner());

        let codec = KeyValueCodec::default();
        let reader = FramedRead::new(r, codec);
        let writer = FramedWrite::new(w, codec);

        let cancel_token = CancellationToken::new();
        let cancel_child = cancel_token.child_token();
        let cancel_guard = cancel_token.drop_guard();

        let opaque_map = Arc::new(std::sync::Mutex::new(OpaqueMap::default()));

        let read_opaque_map = Arc::clone(&opaque_map);
        let read_uuid = opts.id.clone();

        tokio::spawn(async move {
            Client::read_loop(
                reader,
                read_opaque_map,
                ReadLoopOptions {
                    client_id: read_uuid,
                    unsolicited_packet_handler: opts.unsolicited_packet_handler,
                    orphan_handler: opts.orphan_handler,
                    on_read_close_handler: opts.on_read_close_handler,
                    on_close_cancel: cancel_child,
                    disable_decompression: opts.disable_decompression,
                    local_addr,
                    peer_addr,
                },
            )
            .await;
        });

        Self {
            current_opaque: AtomicU32::new(1),
            opaque_map,
            client_id: opts.id,

            on_close_cancel: cancel_guard,

            writer: Mutex::new(writer),

            local_addr,
            peer_addr,

            closed: AtomicBool::new(false),
        }
    }

    async fn dispatch<'a>(
        &self,
        mut packet: RequestPacket<'a>,
        is_persistent: bool,
        response_context: Option<ResponseContext>,
    ) -> error::Result<ClientPendingOp> {
        let (response_tx, response_rx) = mpsc::channel(1);

        let opaque = self.register_handler(SenderContext {
            sender: response_tx,
            is_persistent,
            context: response_context,
        });
        packet.opaque = Some(opaque);
        let op_code = packet.op_code;

        trace!(
            "Writing request on {}. Opcode={}. Opaque={}",
            &self.client_id,
            packet.op_code,
            opaque,
        );

        let mut writer = self.writer.lock().await;
        match writer.send(packet).await {
            Ok(_) => Ok(ClientPendingOp::new(
                opaque,
                self.opaque_map.clone(),
                response_rx,
                is_persistent,
            )),
            Err(e) => {
                debug!(
                    "{} failed to write packet {} {} {}",
                    self.client_id, opaque, op_code, e
                );

                let requests: Arc<std::sync::Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
                {
                    let mut map = requests.lock().unwrap();
                    map.remove(&opaque);
                }

                Err(Error::new_dispatch_error(opaque, op_code, Box::new(e)))
            }
        }
    }

    async fn close(&self) -> error::Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        debug!("Client {} closing", self.client_id);

        let mut close_err = None;
        let mut writer = self.writer.lock().await;
        match writer.close().await {
            Ok(_) => {}
            Err(e) => {
                close_err = Some(e);
            }
        };

        Self::drain_opaque_map(self.opaque_map.clone()).await;

        if let Some(e) = close_err {
            return Err(Error::new_close_error(e.to_string(), Box::new(e)));
        }

        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("Client {} exiting", self.client_id);
    }
}
