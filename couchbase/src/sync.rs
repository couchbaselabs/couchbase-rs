//! Synchronization and Future/Stream abstractions.
use futures::{Async, Future, Poll};
use futures::sync::oneshot::Receiver;
use error::CouchbaseError;

pub struct CouchbaseFuture<T> {
    inner: Receiver<Result<T, CouchbaseError>>,
}

impl<T> CouchbaseFuture<T> {
    pub fn new(rx: Receiver<Result<T, CouchbaseError>>) -> Self {
        CouchbaseFuture { inner: rx }
    }
}

impl<T: Send + 'static> Future for CouchbaseFuture<T> {
    type Item = T;
    type Error = CouchbaseError;

    fn poll(&mut self) -> Poll<T, CouchbaseError> {
        match self.inner.poll().expect("shouldn't be canceled") {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(Err(e)) => Err(e),
            Async::Ready(Ok(e)) => Ok(e.into()),
        }
    }
}
