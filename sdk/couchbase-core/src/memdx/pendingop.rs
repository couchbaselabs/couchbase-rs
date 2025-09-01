use futures::executor::block_on;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use tokio::time::{timeout_at, Instant};

use crate::memdx::client::OpaqueMap;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::{Error, Result};
use crate::memdx::response::TryFromClientResponse;

pub trait PendingOp<T> {
    fn recv(&mut self) -> impl Future<Output = Result<T>>
    where
        T: TryFromClientResponse;
    fn cancel(&mut self, e: CancellationErrorKind) -> impl Future<Output = ()>;
}

pub(crate) trait OpCanceller {
    fn cancel_handler(&mut self, opaque: u32);
}

pub struct ClientPendingOp {
    opaque: u32,
    response_receiver: Receiver<Result<ClientResponse>>,
    opaque_map: Arc<Mutex<OpaqueMap>>,

    is_persistent: bool,
    completed: AtomicBool,
}

impl ClientPendingOp {
    pub(crate) fn new(
        opaque: u32,
        opaque_map: Arc<Mutex<OpaqueMap>>,
        response_receiver: Receiver<Result<ClientResponse>>,
        is_persistent: bool,
    ) -> Self {
        ClientPendingOp {
            opaque,
            opaque_map,
            response_receiver,
            is_persistent,
            completed: AtomicBool::new(false),
        }
    }

    pub async fn recv(&mut self) -> Result<ClientResponse> {
        match self.response_receiver.recv().await {
            Some(r) => {
                if !self.is_persistent {
                    self.completed.store(true, Ordering::SeqCst);
                }

                r
            }
            None => Err(Error::new_cancelled_error(
                CancellationErrorKind::RequestCancelled,
            )),
        }
    }

    pub async fn cancel(&mut self, e: CancellationErrorKind) {
        if self.completed.load(Ordering::SeqCst) {
            return;
        }

        let context = {
            let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
            let mut map = requests.lock().unwrap();

            let t = map.remove(&self.opaque);

            t.map(|map_entry| Arc::clone(&map_entry))
        };

        if let Some(context) = context {
            let sender = &context.sender;

            sender
                .send(Err(Error::new_cancelled_error(e)))
                .await
                .unwrap();
        }
    }
}

impl Drop for ClientPendingOp {
    fn drop(&mut self) {
        block_on(self.cancel(CancellationErrorKind::RequestCancelled));
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

    async fn cancel(&mut self, e: CancellationErrorKind) {
        self.wrapped.cancel(e).await;
    }
}

pub(super) async fn run_op_future_with_deadline<F, T, O>(deadline: Instant, fut: F) -> Result<T>
where
    O: PendingOp<T>,
    F: Future<Output = Result<O>>,
    T: TryFromClientResponse,
{
    let mut op = match timeout_at(deadline, fut).await {
        Ok(op) => op?,
        Err(_e) => {
            return Err(Error::new_cancelled_error(CancellationErrorKind::Timeout));
        }
    };

    match timeout_at(deadline, op.recv()).await {
        Ok(res) => res,
        Err(_e) => {
            op.cancel(CancellationErrorKind::Timeout).await;
            op.recv().await
        }
    }
}
