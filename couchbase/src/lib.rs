#[macro_use]
extern crate log;
extern crate couchbase_sys;
extern crate libc;
extern crate futures;
extern crate volatile;

use couchbase_sys::*;
use std::collections::HashMap;
use std::ffi::CString;
use std::ptr;

use futures::{Async, Future, Poll};
use futures::sync::oneshot::{channel, Sender, Receiver};
use std::panic;

mod ioloop;

pub type CouchbaseError = lcb_error_t;

pub struct Cluster<'a> {
    hosts: &'a str,
    buckets: HashMap<&'a str, Bucket<'a>>,
}

impl<'a> Cluster<'a> {
    pub fn new(hosts: &'a str) -> Cluster<'a> {
        Cluster {
            hosts: hosts,
            buckets: HashMap::new(),
        }
    }

    pub fn from_localhost() -> Cluster<'a> {
        Cluster::new("127.0.0.1")
    }

    pub fn open_bucket(&mut self, name: &'a str, pass: &'a str) -> Result<&Bucket, CouchbaseError> {
        if !self.buckets.contains_key(&name) {
            let bucket = Bucket::open(self.hosts, name, pass);
            if bucket.is_ok() {
                info!("Opening Bucket \"{}\"", name);
                self.buckets.insert(name, bucket.unwrap());
            } else {
                return Err(bucket.err().unwrap());
            }
        } else {
            debug!("Bucket \"{}\" already opened, reusing instance.", name);
        }
        Ok(self.buckets.get(&name).unwrap())
    }
}

impl<'a> Drop for Cluster<'a> {
    fn drop(&mut self) {
        debug!("Couchbase Cluster goes out of scope (Drop).");
        for (name, bucket) in &mut self.buckets {
            debug!("Initiating close on bucket \"{}\"", name);
            bucket.close();
        }
    }
}

pub struct Bucket<'a> {
    instance: lcb_t,
    name: &'a str,
}

impl<'a> Bucket<'a> {
    fn open(hosts: &'a str, name: &'a str, pass: &'a str) -> Result<Bucket<'a>, CouchbaseError> {
        let connstr = CString::new(format!("couchbase://{}/{}", hosts, name)).unwrap();
        let passstr = CString::new(pass).unwrap();

        // let st3: lcb_create_st3 = ;
        let mut cropts = lcb_create_st {
            version: 3,
            v: unsafe { ::std::mem::zeroed() },
        };
        unsafe {
            cropts.v.v3.as_mut().connstr = connstr.as_ptr();
            cropts.v.v3.as_mut().passwd = passstr.as_ptr();
        }
        let mut instance: lcb_t = ptr::null_mut();
        let res = unsafe {
            lcb_create(&mut instance as *mut lcb_t, &cropts as *const lcb_create_st);
            lcb_connect(instance);
            lcb_wait(instance);
            lcb_install_callback3(instance, LCB_CALLBACK_GET as i32, Some(get_callback));
            lcb_get_bootstrap_status(instance)
        };

        match res {
            LCB_SUCCESS => {
                Ok(Bucket {
                    name: name,
                    instance: instance,
                })
            }
            e => Err(e),
        }
    }

    pub fn get(&self, id: &'a str) -> CouchbaseFuture<Document, CouchbaseError> {
        let (tx, rx) = channel();

        let lcb_id = CString::new(id).unwrap();

        let mut cmd_get: lcb_CMDGET = unsafe { ::std::mem::zeroed() };
        cmd_get.key.type_ = LCB_KV_COPY;
        cmd_get.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd_get.key.contig.nbytes = id.len() as usize;

        let tx_boxed = Box::new(tx);

        unsafe {
            lcb_get3(self.instance,
                     Box::into_raw(tx_boxed) as *const std::os::raw::c_void,
                     &cmd_get as *const lcb_CMDGET);
        }

        // TODO: how to do this async non-blocking on polls?
        unsafe {
            lcb_wait(self.instance);
        }

        CouchbaseFuture { inner: rx }
    }

    pub fn close(&mut self) {
        info!("Closing Bucket \"{}\"", self.name);
        unsafe {
            lcb_destroy(self.instance);
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }
}


#[derive(Debug)]
pub struct Document {
    id: String,
    cas: u64,
    content: String,
    expiry: i32,
}

impl Document {
    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn content(&self) -> &String {
        &self.content
    }

    pub fn expiry(&self) -> i32 {
        self.expiry
    }
}

unsafe extern "C" fn get_callback(_: lcb_t, _: i32, resp: *const lcb_RESPBASE) {
    let response = resp as *const lcb_RESPGET;
    let tx = Box::from_raw((*response).cookie as *mut Sender<Result<Document, CouchbaseError>>);

    let content = CString::from_raw((*response).value as *mut i8);
    tx.complete(Ok(Document {
        id: String::from("test"),
        cas: (*response).cas,
        content: content.into_string().unwrap_or(String::from("errror!!!")),
        expiry: 0,
    }));
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
