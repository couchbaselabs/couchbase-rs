extern crate couchbase_sys;
extern crate futures;
extern crate parking_lot;

pub mod bucket;
pub mod cluster;
pub mod document;

pub use document::Document;
pub use bucket::Bucket;
pub use cluster::Cluster;

use couchbase_sys::*;
use futures::{Async, Future, Poll};
use futures::sync::oneshot::Receiver;
use std::panic;

pub type CouchbaseError = lcb_error_t;

pub struct CouchbaseFuture<T, E> {
    inner: Receiver<Result<T, E>>,
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
