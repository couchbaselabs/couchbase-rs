use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;

use log::debug;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant, timeout_at};

use crate::memdx::client::CancellationSender;
use crate::memdx::client::Result;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::Error::Closed;
use crate::memdx::response::TryFromClientResponse;

pub trait PendingOp<T> {
    fn recv(&mut self) -> impl std::future::Future<Output = Result<T>>
    where
        T: TryFromClientResponse;
    fn cancel(&mut self, e: CancellationErrorKind);
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub struct ClientPendingOp {
    opaque: u32,
    cancel_chan: CancellationSender,
    response_receiver: Receiver<Result<ClientResponse>>,
}

impl ClientPendingOp {
    pub(crate) fn new(
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
    pub(crate) fn new(op: ClientPendingOp) -> Self {
        Self {
            wrapped: op,
            _target: PhantomData,
        }
    }
}

impl<T: TryFromClientResponse> PendingOp<T> for StandardPendingOp<T> {
    async fn recv(&mut self) -> Result<T> {
        let packet = self.wrapped.recv().await?;

        T::try_from(packet)
    }

    fn cancel(&mut self, e: CancellationErrorKind) {
        self.wrapped.cancel(e);
    }
}

pub(super) async fn run_op_with_deadline<O, T>(deadline: Instant, op: &mut O) -> Result<T>
where
    O: PendingOp<T>,
    T: TryFromClientResponse,
{
    match timeout_at(deadline, op.recv()).await {
        Ok(res) => res,
        Err(_e) => {
            op.cancel(CancellationErrorKind::Timeout);
            op.recv().await
        }
    }
}
