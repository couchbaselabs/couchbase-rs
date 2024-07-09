use async_trait::async_trait;

use crate::memdx::client::Result;
use crate::memdx::packet::RequestPacket;
use crate::memdx::pendingop::ClientPendingOp;

#[async_trait]
pub trait Dispatcher {
    async fn dispatch(&mut self, packet: RequestPacket) -> Result<ClientPendingOp>;
}
