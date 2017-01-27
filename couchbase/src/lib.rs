extern crate couchbase_sys;
extern crate futures;
extern crate parking_lot;

pub mod bucket;
pub mod cluster;

pub use bucket::Bucket;
pub use cluster::Cluster;

use couchbase_sys::*;
use futures::{Async, Future, Poll};
use futures::sync::oneshot::Receiver;
use std::panic;
use std::str::{from_utf8, Utf8Error};
use std::fmt;
use std::io::Write;

pub type CouchbaseError = lcb_error_t;

pub struct Document {
    id: String,
    cas: u64,
    content: Vec<u8>,
    expiry: i32,
}

impl Document {
    pub fn from_str<'a, S>(id: S, content: &'a str) -> Self
        where S: Into<String>
    {
        let mut vc = Vec::with_capacity(content.len());
        vc.write_all(content.as_bytes()).expect("Could not convert content into vec");
        Self::new(id, vc)
    }

    pub fn new<S>(id: S, content: Vec<u8>) -> Self
        where S: Into<String>
    {
        Document {
            id: id.into(),
            cas: 0,
            content: content,
            expiry: 0,
        }
    }

    pub fn new_with_cas<S>(id: S, content: Vec<u8>, cas: u64) -> Self
        where S: Into<String>
    {
        Document {
            id: id.into(),
            cas: cas,
            content: content,
            expiry: 0,
        }
    }

    pub fn new_with_expiry<S>(id: S, content: Vec<u8>, expiry: i32) -> Self
        where S: Into<String>
    {
        Document {
            id: id.into(),
            cas: 0,
            content: content,
            expiry: expiry,
        }
    }

    pub fn new_with_cas_and_expiry<S>(id: S, content: Vec<u8>, cas: u64, expiry: i32) -> Self
        where S: Into<String>
    {
        Document {
            id: id.into(),
            cas: cas,
            content: content,
            expiry: expiry,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    pub fn content_as_ref(&self) -> &[u8] {
        self.content.as_ref()
    }

    pub fn content(self) -> Vec<u8> {
        self.content
    }

    pub fn content_as_str(&self) -> Result<&str, Utf8Error> {
        from_utf8(self.content_as_ref())
    }

    pub fn expiry(&self) -> i32 {
        self.expiry
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let content = self.content_as_str().unwrap_or("<not utf8 decodable>");
        write!(f,
               "Document {{ id: \"{}\", cas: {}, expiry: {}, content: {} }}",
               self.id,
               self.cas,
               self.expiry,
               content)
    }
}


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
