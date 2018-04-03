//! Bucket-level operations and API.
#![allow(non_upper_case_globals)]

use std::ptr;
use couchbase_sys::*;
use std::sync::Arc;
use parking_lot::Mutex;
use std::thread;
use std::thread::{park, JoinHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use futures::sync::oneshot::channel;
use futures::sync::mpsc::{unbounded, UnboundedSender};
use {CouchbaseError, CouchbaseFuture, CouchbaseStream, Document, N1qlResult, N1qlRow, ViewMeta,
     ViewQuery, ViewResult, ViewRow};
use std;
use std::slice;
use std::mem;
use serde_json;
use std::ffi::CString;

type StreamSender<T> = *mut UnboundedSender<Result<T, CouchbaseError>>;

pub struct Bucket {
    instance: Arc<Mutex<SendPtr<lcb_t>>>,
    io_handle: Mutex<Option<JoinHandle<()>>>,
    io_running: Arc<AtomicBool>,
}

/// Contains all `Bucket`-level Couchbase operations.
impl Bucket {
    pub fn new<'a>(
        cs: &'a str,
        pw: &'a str,
        user: Option<&'a str>,
    ) -> Result<Self, CouchbaseError> {
        let mut instance: lcb_t = ptr::null_mut();

        let connstr = CString::new(cs).unwrap();
        let passstr = CString::new(pw).unwrap();
        let userstr = CString::new(user.unwrap_or("")).unwrap();

        let mut cropts = lcb_create_st {
            version: 3,
            v: unsafe { ::std::mem::zeroed() },
        };

        let boot_result = unsafe {
            cropts.v.v3.connstr = connstr.as_ptr();
            if user.is_some() {
                cropts.v.v3.username = userstr.as_ptr();
            }
            cropts.v.v3.passwd = passstr.as_ptr();

            lcb_create(&mut instance, &cropts);

            let mut enable = true;
            let cntl_res = lcb_cntl(
                instance,
                LCB_CNTL_SET as i32,
                LCB_CNTL_DETAILED_ERRCODES as i32,
                &mut enable as *mut bool as *mut std::os::raw::c_void,
            );
            if cntl_res != lcb_error_t_LCB_SUCCESS {
                return Err(cntl_res.into());
            }

            lcb_connect(instance);
            lcb_wait(instance);
            lcb_get_bootstrap_status(instance)
        };

        if boot_result != lcb_error_t_LCB_SUCCESS {
            return Err(boot_result.into());
        }

        // install the generic callbacks
        unsafe {
            lcb_install_callback3(
                instance,
                lcb_CALLBACKTYPE_LCB_CALLBACK_GET as i32,
                Some(get_callback),
            );
            lcb_install_callback3(
                instance,
                lcb_CALLBACKTYPE_LCB_CALLBACK_STORE as i32,
                Some(store_callback),
            );
            lcb_install_callback3(
                instance,
                lcb_CALLBACKTYPE_LCB_CALLBACK_REMOVE as i32,
                Some(remove_callback),
            );
        }

        let mt_instance = Arc::new(Mutex::new(SendPtr {
            inner: Some(instance),
        }));
        let io_running = Arc::new(AtomicBool::new(true));

        let io_instance = mt_instance.clone();
        let still_running = io_running.clone();
        let handle = thread::Builder::new()
            .name("io".into())
            .spawn(move || loop {
                park();
                if !still_running.load(Ordering::Acquire) {
                    break;
                }
                let guard = io_instance.lock();
                let instance = guard.inner.unwrap();
                unsafe { lcb_wait(instance) };
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

    /// Fetch a `Document` from the `Bucket`.
    pub fn get<D, S>(&self, id: S) -> CouchbaseFuture<D>
    where
        S: Into<String>,
        D: Document,
    {
        let (tx, rx) = channel();

        let idm = id.into();
        let idm_len = idm.len();
        let lcb_id = CString::new(idm).unwrap();
        let mut cmd: lcb_CMDGET = unsafe { ::std::mem::zeroed() };
        cmd.key.type_ = lcb_KVBUFTYPE_LCB_KV_COPY;
        cmd.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd.key.contig.nbytes = idm_len as usize;

        let mut tx_boxed = Box::new(Some(tx));
        let callback = move |res: &lcb_RESPGET| {
            let result = match res.rc {
                lcb_error_t_LCB_SUCCESS => {
                    let lcb_id =
                        unsafe { slice::from_raw_parts(res.key as *const u8, res.nkey).to_owned() };
                    let id =
                        String::from_utf8(lcb_id).expect("Document ID is not UTF-8 compatible!");
                    let content = unsafe {
                        slice::from_raw_parts(res.value as *const u8, res.nvalue).to_owned()
                    };
                    Ok(D::create(id, Some(res.cas), Some(content), None))
                }
                rc => Err(rc.into()),
            };
            let _ = tx_boxed.take().unwrap().send(result);
        };

        let callback_boxed: Box<Box<FnMut(&lcb_RESPGET)>> = Box::new(Box::new(callback));
        unsafe {
            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();
            lcb_get3(
                instance,
                Box::into_raw(callback_boxed) as *const std::os::raw::c_void,
                &cmd as *const lcb_CMDGET,
            );
        }

        self.unpark_io();
        CouchbaseFuture::new(rx)
    }

    /// Insert a `Document` into the `Bucket`.
    pub fn insert<D>(&self, document: D) -> CouchbaseFuture<D>
    where
        D: Document,
    {
        self.store(document, lcb_storage_t_LCB_ADD)
    }

    /// Upsert a `Document` into the `Bucket`.
    pub fn upsert<D>(&self, document: D) -> CouchbaseFuture<D>
    where
        D: Document,
    {
        self.store(document, lcb_storage_t_LCB_UPSERT)
    }

    /// Replace a `Document` in the `Bucket`.
    pub fn replace<D>(&self, document: D) -> CouchbaseFuture<D>
    where
        D: Document,
    {
        self.store(document, lcb_storage_t_LCB_REPLACE)
    }

    fn store<D>(&self, document: D, operation: lcb_storage_t) -> CouchbaseFuture<D>
    where
        D: Document,
    {
        let (tx, rx) = channel();

        let lcb_id = CString::new(document.id()).unwrap();
        let mut cmd: lcb_CMDSTORE = unsafe { ::std::mem::zeroed() };
        cmd.operation = operation;
        cmd.exptime = document.expiry().unwrap_or(0);
        cmd.key.type_ = lcb_KVBUFTYPE_LCB_KV_COPY;
        cmd.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd.key.contig.nbytes = document.id().len() as usize;
        cmd.flags = document.flags();

        let mut tx_boxed = Box::new(Some(tx));
        let callback = move |res: &lcb_RESPBASE| {
            let result = match res.rc {
                lcb_error_t_LCB_SUCCESS => {
                    let lcb_id = unsafe {
                        slice::from_raw_parts((*res).key as *const u8, (*res).nkey).to_owned()
                    };
                    let id =
                        String::from_utf8(lcb_id).expect("Document ID is not UTF-8 compatible!");
                    Ok(D::create(id, Some(res.cas), None, None))
                }
                rc => Err(rc.into()),
            };
            let _ = tx_boxed.take().unwrap().send(result);
        };

        let content: Vec<u8> = document.content_into_vec().expect("No content found");
        let content_len = content.len();
        let lcb_content = CString::new(content).unwrap();
        cmd.value.vtype = lcb_KVBUFTYPE_LCB_KV_COPY;
        unsafe {
            cmd.value.u_buf.contig.bytes = lcb_content.into_raw() as *const std::os::raw::c_void;
            cmd.value.u_buf.contig.nbytes = content_len as usize;
        }

        let callback_boxed: Box<Box<FnMut(&lcb_RESPBASE)>> = Box::new(Box::new(callback));
        unsafe {
            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();

            lcb_store3(
                instance,
                Box::into_raw(callback_boxed) as *const std::os::raw::c_void,
                &cmd as *const lcb_CMDSTORE,
            );
        }

        self.unpark_io();
        CouchbaseFuture::new(rx)
    }

    pub fn remove<D, S>(&self, id: S) -> CouchbaseFuture<D>
    where
        S: Into<String>,
        D: Document,
    {
        let (tx, rx) = channel();

        let idm = id.into();
        let idm_len = idm.len();
        let lcb_id = CString::new(idm).unwrap();
        let mut cmd: lcb_CMDREMOVE = unsafe { ::std::mem::zeroed() };
        cmd.key.type_ = lcb_KVBUFTYPE_LCB_KV_COPY;
        cmd.key.contig.bytes = lcb_id.into_raw() as *const std::os::raw::c_void;
        cmd.key.contig.nbytes = idm_len as usize;

        let mut tx_boxed = Box::new(Some(tx));
        let callback = move |res: &lcb_RESPBASE| {
            let result = match res.rc {
                lcb_error_t_LCB_SUCCESS => {
                    let lcb_id = unsafe {
                        slice::from_raw_parts((*res).key as *const u8, (*res).nkey).to_owned()
                    };
                    let id =
                        String::from_utf8(lcb_id).expect("Document ID is not UTF-8 compatible!");
                    Ok(D::create(id, Some(res.cas), None, None))
                }
                rc => Err(rc.into()),
            };
            let _ = tx_boxed.take().unwrap().send(result);
        };

        let callback_boxed: Box<Box<FnMut(&lcb_RESPBASE)>> = Box::new(Box::new(callback));
        unsafe {
            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();
            lcb_remove3(
                instance,
                Box::into_raw(callback_boxed) as *const std::os::raw::c_void,
                &cmd as *const lcb_CMDREMOVE,
            );
        }

        self.unpark_io();
        CouchbaseFuture::new(rx)
    }

    pub fn query_n1ql<S>(&self, query: S) -> CouchbaseStream<N1qlResult>
    where
        S: Into<String>,
    {
        let (tx, rx) = unbounded();

        let params = unsafe { lcb_n1p_new() };
        let mut cmd: lcb_CMDN1QL = unsafe { ::std::mem::zeroed() };
        cmd.callback = Some(n1ql_callback);

        let query = query.into();
        let query_length = query.len();
        let cquery = CString::new(query).unwrap();

        let tx_boxed = Box::new(tx);
        unsafe {
            lcb_n1p_setquery(
                params,
                cquery.as_ptr(),
                query_length,
                LCB_N1P_QUERY_STATEMENT as i32,
            );
            lcb_n1p_mkcmd(params, &mut cmd);

            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();
            lcb_n1ql_query(
                instance,
                Box::into_raw(tx_boxed) as *const std::os::raw::c_void,
                &cmd,
            );
        }

        self.unpark_io();
        CouchbaseStream::new(rx)
    }

    pub fn query_view(&self, query: ViewQuery) -> CouchbaseStream<ViewResult> {
        let (tx, rx) = unbounded();

        let cdesign = CString::new(query.design().to_owned()).unwrap();
        let cview = CString::new(query.view().to_owned()).unwrap();
        let coptions = CString::new(query.params()).unwrap();

        let mut cmd: lcb_CMDVIEWQUERY = unsafe { ::std::mem::zeroed() };
        let tx_boxed = Box::new(tx);
        unsafe {
            lcb_view_query_initcmd(
                &mut cmd,
                cdesign.as_ptr(),
                cview.as_ptr(),
                coptions.as_ptr(),
                Some(view_callback),
            );

            let guard = self.instance.lock();
            let instance = guard.inner.unwrap();
            lcb_view_query(
                instance,
                Box::into_raw(tx_boxed) as *const std::os::raw::c_void,
                &cmd,
            );
        }

        self.unpark_io();
        CouchbaseStream::new(rx)
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

unsafe extern "C" fn get_callback(_instance: lcb_t, _cbtype: i32, res: *const lcb_RESPBASE) {
    let closure: &mut Box<FnMut(&lcb_RESPGET)> = mem::transmute((*res).cookie);
    closure(&*(res as *const lcb_RESPGET));
}

unsafe extern "C" fn store_callback(_instance: lcb_t, _cbtype: i32, res: *const lcb_RESPBASE) {
    let closure: &mut Box<FnMut(&lcb_RESPBASE)> = mem::transmute((*res).cookie);
    closure(&*res);
}

unsafe extern "C" fn remove_callback(_instance: lcb_t, _cbtype: i32, res: *const lcb_RESPBASE) {
    let closure: &mut Box<FnMut(&lcb_RESPBASE)> = mem::transmute((*res).cookie);
    closure(&*res);
}

unsafe extern "C" fn n1ql_callback(_instance: lcb_t, _cbtype: i32, res: *const lcb_RESPN1QL) {
    let tx = Box::from_raw((*res).cookie as StreamSender<N1qlResult>);
    let more_to_come = ((*res).rflags as u32 & lcb_RESPFLAGS_LCB_RESP_F_FINAL as u32) == 0;

    let result = if (*res).rc == lcb_error_t_LCB_SUCCESS {
        let lcb_row = slice::from_raw_parts((*res).row as *const u8, (*res).nrow);
        let result = if more_to_come {
            N1qlResult::Row(N1qlRow::new(
                String::from_utf8(lcb_row.to_owned()).expect("N1QL Row failed UTF-8 validation!"),
            ))
        } else {
            N1qlResult::Meta(serde_json::from_slice(lcb_row).expect("N1QL Meta decoding failed!"))
        };
        Ok(result)
    } else {
        Err((*res).rc.into())
    };

    match tx.unbounded_send(result) {
        Ok(_) => {}
        Err(e) => warn!("Could not send N1qlResult into Stream! {}", e),
    }
    if more_to_come {
        Box::into_raw(tx);
    }
}

unsafe extern "C" fn view_callback(_instance: lcb_t, _cbtype: i32, res: *const lcb_RESPVIEWQUERY) {
    let tx = Box::from_raw((*res).cookie as StreamSender<ViewResult>);
    let more_to_come = ((*res).rflags as u32 & lcb_RESPFLAGS_LCB_RESP_F_FINAL as u32) == 0;

    let result = if (*res).rc == lcb_error_t_LCB_SUCCESS {
        let lcb_value = slice::from_raw_parts((*res).value as *const u8, (*res).nvalue);
        let value =
            String::from_utf8(lcb_value.to_owned()).expect("View Row failed UTF-8 validation!");
        let result = if more_to_come {
            let lcb_key = slice::from_raw_parts((*res).key as *const u8, (*res).nkey);
            let lcb_docid = slice::from_raw_parts((*res).docid as *const u8, (*res).ndocid);
            ViewResult::Row(ViewRow {
                id: String::from_utf8(lcb_docid.to_owned())
                    .expect("View DocId failed UTF-8 validation!"),
                value: value,
                key: String::from_utf8(lcb_key.to_owned())
                    .expect("View Key failed UTF-8 validation!"),
            })
        } else {
            ViewResult::Meta(ViewMeta { inner: value })
        };
        Ok(result)
    } else {
        Err((*res).rc.into())
    };

    match tx.unbounded_send(result) {
        Ok(_) => {}
        Err(e) => warn!("Could not send ViewResult into Stream! {}", e),
    }
    if more_to_come {
        Box::into_raw(tx);
    }
}
