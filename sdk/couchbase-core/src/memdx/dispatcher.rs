use crate::memdx::client::Result;
use crate::memdx::packet::RequestPacket;
use crate::memdx::pendingop::ClientPendingOp;

pub trait Dispatcher {
    fn dispatch(
        &mut self,
        packet: RequestPacket,
    ) -> impl std::future::Future<Output = Result<ClientPendingOp>> + Send;
}
