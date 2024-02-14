use crate::memdx::client::Result;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::{ClientPendingOp, PendingOp};

pub type DispatchFn = dyn (Fn(Result<ResponsePacket>) -> bool) + Send + Sync;

pub trait Dispatcher {
    async fn dispatch(
        &mut self,
        packet: RequestPacket,
        handler: impl (Fn(Result<ResponsePacket>) -> bool) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>;
}
