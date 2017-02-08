//! Synchronization and Future/Stream abstractions.
use futures::{Async, Future, Poll, Stream};
use futures::sync::oneshot::Receiver;
use futures::sync::mpsc::UnboundedReceiver;
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
        match self.inner.poll().expect("CouchbaseFuture shouldn't be canceled!") {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(Err(e)) => Err(e),
            Async::Ready(Ok(e)) => Ok(e.into()),
        }
    }
}

pub struct CouchbaseStream<T> {
    inner: UnboundedReceiver<Result<T, CouchbaseError>>,
}

impl<T> CouchbaseStream<T> {
    pub fn new(rx: UnboundedReceiver<Result<T, CouchbaseError>>) -> Self {
        CouchbaseStream { inner: rx }
    }
}

impl<T: Send + 'static> Stream for CouchbaseStream<T> {
    type Item = T;
    type Error = CouchbaseError;

    fn poll(&mut self) -> Poll<Option<T>, CouchbaseError> {
        match self.inner.poll().expect("CouchbaseStream shouldn't be canceled!") {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(Some(Ok(e))) => Ok(Async::Ready(Some(e))),
            Async::Ready(Some(Err(e))) => Err(e),
            Async::Ready(None) => Ok(Async::Ready(None)),
        }
    }
}
