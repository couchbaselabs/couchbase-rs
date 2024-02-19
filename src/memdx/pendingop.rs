use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::CancellationSender;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::op_bootstrap::OpAuthEncoder;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::request::SASLStepRequest;
use crate::memdx::response::{SASLAuthResponse, TryFromResponsePacket};
use log::debug;
use tokio::sync::mpsc::Receiver;

pub trait PendingOp {
    fn cancel(&mut self, e: CancellationErrorKind);
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub(crate) struct ClientPendingOp {
    opaque: u32,
    cancel_chan: CancellationSender,
    response_receiver: Receiver<Result<ResponsePacket>>,
}

impl ClientPendingOp {
    pub fn new(
        opaque: u32,
        cancel_chan: CancellationSender,
        response_receiver: Receiver<Result<ResponsePacket>>,
    ) -> Self {
        ClientPendingOp {
            opaque,
            cancel_chan,
            response_receiver,
        }
    }

    pub async fn recv(&mut self) -> Result<ResponsePacket> {
        // TODO: unwrap
        self.response_receiver.recv().await.unwrap()
    }
}

impl PendingOp for ClientPendingOp {
    fn cancel(&mut self, e: CancellationErrorKind) {
        match self.cancel_chan.send((self.opaque, e)) {
            Ok(_) => {}
            Err(e) => {
                debug!("Failed to send cancel to channel {}", e);
            }
        };
    }
}

pub struct StandardPendingOp {
    wrapped: ClientPendingOp,
}

impl StandardPendingOp {
    pub fn new(op: ClientPendingOp) -> Self {
        Self { wrapped: op }
    }

    pub async fn recv<T: TryFromResponsePacket>(&mut self) -> Result<T> {
        let packet = self.wrapped.recv().await?;

        T::try_from(packet)
    }
}

pub(crate) struct SASLAuthScramPendingOp<'a, E: OpAuthEncoder, D: Dispatcher> {
    initial_op: StandardPendingOp,
    dispatcher: &'a mut D,
    encoder: &'a E,
}

impl<'a, E: OpAuthEncoder, D: Dispatcher> SASLAuthScramPendingOp<'a, E, D> {
    pub fn new(initial_op: StandardPendingOp, encoder: &'a E, dispatcher: &'a mut D) -> Self {
        Self {
            initial_op,
            dispatcher,
            encoder,
        }
    }

    pub async fn recv(&mut self) -> Result<()> {
        let resp = self.initial_op.recv::<SASLAuthResponse>().await?;

        if !resp.needs_more_steps {
            return Ok(());
        }

        self.encoder
            .sasl_step(
                self.dispatcher,
                SASLStepRequest {
                    auth_mechanism: AuthMechanism::Plain,
                    payload: vec![],
                },
            )
            .await
            .unwrap();

        Ok(())
    }
}
