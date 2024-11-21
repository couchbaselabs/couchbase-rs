use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{timeout_at, Instant};

use crate::memdx::client::OpaqueMap;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::Result;
use crate::memdx::error::{CancellationErrorKind, ErrorKind};
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
}

impl ClientPendingOp {
    pub(crate) fn new(
        opaque: u32,
        opaque_map: Arc<Mutex<OpaqueMap>>,
        response_receiver: Receiver<Result<ClientResponse>>,
    ) -> Self {
        ClientPendingOp {
            opaque,
            opaque_map,
            response_receiver,
        }
    }

    pub async fn recv(&mut self) -> Result<ClientResponse> {
        match self.response_receiver.recv().await {
            Some(r) => r,
            None => Err(ErrorKind::Cancelled(CancellationErrorKind::RequestCancelled).into()),
        }
    }

    pub async fn cancel(&mut self, e: CancellationErrorKind) {
        // match self.cancel_chan.send((self.opaque, e)) {
        //     Ok(_) => {}
        //     Err(e) => {
        //         debug!("Failed to send cancel to channel {}", e);
        //     }
        // };
        let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().await;

        let t = map.remove(&self.opaque);

        if let Some(map_entry) = t {
            let context = Arc::clone(&map_entry);
            let sender = &context.sender;
            drop(map);

            sender
                .send(Err(ErrorKind::Cancelled(e).into()))
                .await
                .unwrap();
        } else {
            drop(map);
        }

        drop(requests);
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
            return Err(ErrorKind::Cancelled(CancellationErrorKind::Timeout).into());
        }
    };

    match timeout_at(deadline, op.recv()).await {
        Ok(res) => res,
        Err(_e) => {
            op.cancel(CancellationErrorKind::Timeout);
            op.recv().await
        }
    }
}
