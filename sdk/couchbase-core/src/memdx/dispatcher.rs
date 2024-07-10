use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::memdx::client::Result;
use crate::memdx::connection::Connection;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

#[derive(Debug)]
pub struct DispatcherOptions {
    pub orphan_handler: UnboundedSender<ResponsePacket>,
    pub on_connection_close_handler: Option<oneshot::Sender<Result<()>>>,
}

#[async_trait]
pub trait Dispatcher {
    fn new(conn: Connection, opts: DispatcherOptions) -> Self;
    async fn dispatch(&mut self, packet: RequestPacket) -> Result<ClientPendingOp>;
    async fn close(mut self) -> Result<()>;
}
