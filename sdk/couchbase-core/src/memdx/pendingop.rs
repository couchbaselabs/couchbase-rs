/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use tokio::time::{timeout_at, Instant};

use crate::memdx::client::{OpaqueMap, SenderContext};
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::{Error, Result};
use crate::memdx::response::TryFromClientResponse;

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

    pub async fn cancel(&mut self, e: CancellationErrorKind) -> bool {
        let context = self.cancel_op();

        if let Some(context) = context {
            let sender = &context.sender;

            sender
                .send(Err(Error::new_cancelled_error(e)))
                .await
                .unwrap();

            true
        } else {
            false
        }
    }

    fn cancel_op(&mut self) -> Option<SenderContext> {
        if self.completed.load(Ordering::SeqCst) {
            return None;
        }

        let requests: Arc<Mutex<OpaqueMap>> = Arc::clone(&self.opaque_map);
        let mut map = requests.lock().unwrap();

        map.remove(&self.opaque)
    }
}

impl Drop for ClientPendingOp {
    fn drop(&mut self) {
        // We don't need to send a cancellation error on the sender here, we own the receiver
        // and if we've been dropped then the receiver is gone with us.
        self.cancel_op();
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

impl<T: TryFromClientResponse> StandardPendingOp<T> {
    pub async fn recv(&mut self) -> Result<T> {
        let packet = self.wrapped.recv().await?;

        T::try_from(packet)
    }

    pub async fn cancel(&mut self, e: CancellationErrorKind) -> bool {
        self.wrapped.cancel(e).await
    }
}

pub(super) async fn run_op_future_with_deadline<F, T>(deadline: Instant, fut: F) -> Result<T>
where
    F: Future<Output = Result<StandardPendingOp<T>>>,
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
            if op.cancel(CancellationErrorKind::Timeout).await {
                return Err(Error::new_cancelled_error(CancellationErrorKind::Timeout));
            };

            op.recv().await
        }
    }
}
