use std::ptr;
use couchbase_sys::*;
use std::ffi::CString;
use std::sync::Arc;
use parking_lot::Mutex;
use std::thread;
use std::thread::{park, JoinHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use futures::sync::oneshot::{channel, Sender};
use ::CouchbaseFuture;
use ::Document;
use ::CouchbaseError;
use std;
use std::io::Write;

pub struct Bucket {
    instance: Arc<Mutex<SendPtr<lcb_t>>>,
    io_handle: Mutex<Option<JoinHandle<()>>>,
    io_running: Arc<AtomicBool>,
}

impl Bucket {
    pub fn new<'a>(cs: &'a str, pw: &'a str) -> Result<Self, CouchbaseError> {
        let mut instance: lcb_t = ptr::null_mut();

        let connstr = CString::new(cs).unwrap();
        let passstr = CString::new(pw).unwrap();

        let mut cropts = lcb_create_st {
            version: 3,
            v: unsafe { ::std::mem::zeroed() },
        };

        let boot_result = unsafe {
            cropts.v.v3.as_mut().connstr = connstr.as_ptr();
            cropts.v.v3.as_mut().passwd = passstr.as_ptr();

            lcb_create(&mut instance, &cropts);
            lcb_connect(instance);
            lcb_wait(instance);
            lcb_get_bootstrap_status(instance)
        };

        if boot_result != LCB_SUCCESS {
            return Err(boot_result);
            // panic!("Couldn't connect. Result {:?}", boot_result);
        }

        // install the generic callbacks
        unsafe {
            lcb_install_callback3(instance, LCB_CALLBACK_GET as i32, Some(get_callback));
            lcb_install_callback3(instance, LCB_CALLBACK_STORE as i32, Some(store_callback));
        }

        let mt_instance = Arc::new(Mutex::new(SendPtr { inner: Some(instance) }));
        let io_running = Arc::new(AtomicBool::new(true));

        let io_instance = mt_instance.clone();
        let still_running = io_running.clone();
        let handle = thread::Builder::new()
            .name("io".into())
            .spawn(move || {
                loop {
                    park();
                    if !still_running.load(Ordering::Acquire) {
                        break;
                    }
                    let guard = io_instance.lock();
                    let instance = guard.inner.unwrap();
                    unsafe { lcb_wait(instance) };
                }
            })
            .unwrap();

        Ok(Bucket {
            instance: mt_instance,
            io_handle: Mutex::new(Some(handle)),
            io_running: io_running,
        })
    }

    fn unpark_io(&self) {
        let guard = self.io_handle.lock();
        guard.as_ref().unwrap().thread().unpark();
    }

    pub fn get<S>(&self, id: S) -> CouchbaseFuture<Option<Document>, CouchbaseError>
        where S: Into<String>
    {
        let (tx, rx) = channel();

        let idm = id.into();
        let idm_len = idm.len();
        let lcb_id = CString::new(idm).unwrap();
        let mut cmd: lcb_CMDGET = unsafe { ::std::mem::zeroed() };
        cmd.key.type_ = LCB_KV_COPY;
        cmd.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd.key.contig.nbytes = idm_len as usize;

        let tx_boxed = Box::new(tx);

        unsafe {
            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();
            lcb_get3(instance,
                     Box::into_raw(tx_boxed) as *const std::os::raw::c_void,
                     &cmd as *const lcb_CMDGET);
        }

        self.unpark_io();
        CouchbaseFuture { inner: rx }
    }

    pub fn insert(&self, document: Document) -> CouchbaseFuture<Document, CouchbaseError> {
        self.store(document, LCB_ADD)
    }

    pub fn upsert(&self, document: Document) -> CouchbaseFuture<Document, CouchbaseError> {
        self.store(document, LCB_UPSERT)
    }

    pub fn replace(&self, document: Document) -> CouchbaseFuture<Document, CouchbaseError> {
        self.store(document, LCB_REPLACE)
    }

    fn store(&self,
             document: Document,
             operation: lcb_storage_t)
             -> CouchbaseFuture<Document, CouchbaseError> {
        let (tx, rx) = channel();

        let lcb_id = CString::new(document.id()).unwrap();
        let mut cmd: lcb_CMDSTORE = unsafe { ::std::mem::zeroed() };
        cmd.operation = operation;
        cmd.key.type_ = LCB_KV_COPY;
        cmd.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd.key.contig.nbytes = document.id().len() as usize;

        let content = document.content();
        let content_len = content.len();
        let lcb_content = CString::new(content).unwrap();
        cmd.value.vtype = LCB_KV_COPY;
        unsafe {
            cmd.value.u_buf.contig.as_mut().bytes =
                lcb_content.into_raw() as *const std::os::raw::c_void;
            cmd.value.u_buf.contig.as_mut().nbytes = content_len as usize;
        }
        let tx_boxed = Box::new(tx);

        unsafe {
            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();

            lcb_store3(instance,
                       Box::into_raw(tx_boxed) as *const std::os::raw::c_void,
                       &cmd as *const lcb_CMDSTORE);
        }

        self.unpark_io();
        CouchbaseFuture { inner: rx }
    }
}

impl Drop for Bucket {
    fn drop(&mut self) {
        // stop the IO loop
        self.io_running.clone().store(false, Ordering::Release);
        self.unpark_io();

        // wait until the IO thread is dead
        let mut unlocked_handle = self.io_handle.lock();
        unlocked_handle.take().unwrap().join().unwrap();

        // finally, destroy the instance
        let mut guard = self.instance.lock();
        let instance = guard.inner.take().unwrap();
        unsafe { lcb_destroy(instance) };
    }
}

struct SendPtr<T> {
    inner: Option<T>,
}

unsafe impl<T> Send for SendPtr<T> {}

unsafe extern "C" fn get_callback(_: lcb_t, _: i32, rb: *const lcb_RESPBASE) {
    let response = *(rb as *const lcb_RESPGET);
    let tx = Box::from_raw(response.cookie as *mut Sender<Result<Option<Document>,
                                                                 CouchbaseError>>);
    if response.rc == LCB_SUCCESS {
        let lcb_content = std::slice::from_raw_parts(response.value as *const u8, response.nvalue);
        let mut content = Vec::with_capacity(lcb_content.len());
        content.write_all(lcb_content).expect("Could not copy content from lcb into owned vec!");

        let lcb_id = std::slice::from_raw_parts(response.key as *const u8, response.nkey);
        let mut id_vec = Vec::with_capacity(lcb_id.len());
        id_vec.write_all(lcb_id).expect("Could not copy document ID from lcb into owned vec!");

        tx.complete(Ok(Some(Document {
            id: String::from_utf8(id_vec).expect("Document ID is not UTF8 compatible!"),
            cas: response.cas.clone(),
            content: content,
            expiry: 0,
        })));
    } else if response.rc == LCB_KEY_ENOENT {
        tx.complete(Ok(None));
    } else {
        tx.complete(Err(response.rc));
    }
}

unsafe extern "C" fn store_callback(_: lcb_t, _: i32, rb: *const lcb_RESPBASE) {
    let response = *rb;
    let tx = Box::from_raw(response.cookie as *mut Sender<Result<Document, CouchbaseError>>);

    if response.rc == LCB_SUCCESS {
        let lcb_id = std::slice::from_raw_parts(response.key as *const u8, response.nkey);
        let mut id_vec = Vec::with_capacity(lcb_id.len());
        id_vec.write_all(lcb_id).expect("Could not copy document ID from lcb into owned vec!");

        tx.complete(Ok(Document {
            id: String::from_utf8(id_vec).expect("Document ID is not UTF8 compatible!"),
            cas: response.cas.clone(),
            content: vec![],
            expiry: 0,
        }));
    } else {
        tx.complete(Err(response.rc));
    }
}
