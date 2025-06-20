use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::memdx::client::ResponseContext;
use crate::memdx::connection::ConnectionType;
use crate::memdx::error::Result;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::ClientPendingOp;

pub type UnsolicitedPacketHandler =
    Arc<dyn Fn(ResponsePacket) -> BoxFuture<'static, ()> + Send + Sync>;
pub type OrphanResponseHandler = Arc<dyn Fn(ResponsePacket) + Send + Sync>;
pub type OnConnectionCloseHandler = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

pub struct DispatcherOptions {
    pub unsolicited_packet_handler: UnsolicitedPacketHandler,
    pub orphan_handler: OrphanResponseHandler,
    pub on_connection_close_handler: OnConnectionCloseHandler,
    pub disable_decompression: bool,
    pub id: String,
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
    fn new(conn: ConnectionType, opts: DispatcherOptions) -> Self;
    async fn dispatch<'a>(
        &self,
        packet: RequestPacket<'a>,
        response_context: Option<ResponseContext>,
    ) -> Result<ClientPendingOp>;
    async fn close(&self) -> Result<()>;
}
