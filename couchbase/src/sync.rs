//! Synchronization and Future/Stream abstractions.
use futures::{Async, Future, Poll, Stream};
use futures::channel::oneshot::Receiver;
use futures::channel::mpsc::UnboundedReceiver;
use error::CouchbaseError;
use futures::task::Context;

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

    fn poll(&mut self, cx: &mut Context) -> Result<Async<Self::Item>, Self::Error> {
        match self.inner
            .poll(cx)
            .expect("CouchbaseFuture shouldn't be canceled!")
        {
            Async::Pending => Ok(Async::Pending),
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

    fn poll_next(&mut self, cx: &mut Context) -> Result<Async<Option<Self::Item>>, Self::Error> {
        match self.inner
            .poll_next(cx)
            .expect("CouchbaseStream shouldn't be canceled!")
        {
            Async::Pending => Ok(Async::Pending),
            Async::Ready(Some(Ok(e))) => Ok(Async::Ready(Some(e))),
            Async::Ready(Some(Err(e))) => Err(e),
            Async::Ready(None) => Ok(Async::Ready(None)),
        }
    }
}
