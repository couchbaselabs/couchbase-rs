use std::marker::PhantomData;

use log::debug;
use tokio::sync::mpsc::Receiver;

use crate::memdx::client::CancellationSender;
use crate::memdx::client::Result;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::Error::Closed;
use crate::memdx::response::TryFromClientResponse;

pub trait PendingOp {
    fn cancel(&mut self, e: CancellationErrorKind);
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub(crate) struct ClientPendingOp {
    opaque: u32,
    cancel_chan: CancellationSender,
    response_receiver: Receiver<Result<ClientResponse>>,
}

impl ClientPendingOp {
    pub fn new(
        opaque: u32,
        cancel_chan: CancellationSender,
        response_receiver: Receiver<Result<ClientResponse>>,
    ) -> Self {
        ClientPendingOp {
            opaque,
            cancel_chan,
            response_receiver,
        }
    }

    pub async fn recv(&mut self) -> Result<ClientResponse> {
        match self.response_receiver.recv().await {
            Some(r) => r,
            None => Err(Closed),
        }
    }

    pub fn cancel(&mut self, e: CancellationErrorKind) {
        match self.cancel_chan.send((self.opaque, e)) {
            Ok(_) => {}
            Err(e) => {
                debug!("Failed to send cancel to channel {}", e);
            }
        };
    }
}

pub struct StandardPendingOp<TryFromClientResponse> {
    wrapped: ClientPendingOp,
    _target: PhantomData<TryFromClientResponse>,
}

impl<T: TryFromClientResponse> StandardPendingOp<T> {
    pub fn new(op: ClientPendingOp) -> Self {
        Self {
            wrapped: op,
            _target: PhantomData,
        }
    }

    pub async fn recv(&mut self) -> Result<T> {
        let packet = self.wrapped.recv().await?;

        T::try_from(packet)
    }

    pub fn cancel(&mut self, e: CancellationErrorKind) {
        self.wrapped.cancel(e);
    }
}
