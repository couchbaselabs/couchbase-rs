use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::memdx::connection::Connection;
use crate::memdx::error::Result;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

pub type OrphanResponseHandler = Arc<dyn Fn(ResponsePacket) + Send + Sync>;
pub type OnConnectionCloseHandler = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

pub struct DispatcherOptions {
    pub orphan_handler: OrphanResponseHandler,
    pub on_connection_close_handler: OnConnectionCloseHandler,
    pub disable_decompression: bool,
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
    fn new(conn: Connection, opts: DispatcherOptions) -> Self;
    async fn dispatch(&self, packet: RequestPacket) -> Result<ClientPendingOp>;
    async fn close(&self) -> Result<()>;
}
