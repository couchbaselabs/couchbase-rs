//! Synchronization and Future/Stream abstractions.
use futures::{Async, Future, Poll};
use futures::sync::oneshot::Receiver;
use std::panic;

pub struct CouchbaseFuture<T, E> {
    inner: Receiver<Result<T, E>>,
}

impl<T, E> CouchbaseFuture<T, E> {
    pub fn new(rx: Receiver<Result<T, E>>) -> Self {
        CouchbaseFuture { inner: rx }
    }
}

impl<T: Send + 'static, E: Send + 'static> Future for CouchbaseFuture<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<T, E> {
        match self.inner.poll().expect("shouldn't be canceled") {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(Err(e)) => panic::resume_unwind(Box::new(e)),
            Async::Ready(Ok(e)) => Ok(Async::Ready(e)),
        }
    }
}
