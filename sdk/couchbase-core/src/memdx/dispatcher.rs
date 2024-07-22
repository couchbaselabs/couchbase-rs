use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::memdx::connection::Connection;
use crate::memdx::error::Result;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

#[derive(Debug)]
pub struct DispatcherOptions {
    pub orphan_handler: Arc<UnboundedSender<ResponsePacket>>,
    pub on_connection_close_handler: Option<oneshot::Sender<Result<()>>>,
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
    fn new(conn: Connection, opts: DispatcherOptions) -> Self;
    async fn dispatch(&self, packet: RequestPacket) -> Result<ClientPendingOp>;
    async fn close(&self) -> Result<()>;
}
