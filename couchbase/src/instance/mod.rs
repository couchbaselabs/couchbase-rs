//! A safe wrapper around the unsafe libcouchbase instance.
//!
//! Warning: you'll find lots of unsafe blocks in this file, but hopefully less
//! of them in the other files as as a result. If something segfaults, this is
//! likely the place to look.

mod request;

use crate::options::GetOptions;
use crate::result::{GetResult, MutationResult};
use request::{GetRequest, InstanceRequest, UpsertRequest};

use couchbase_sys::*;
use futures::sync::oneshot;
use futures::Future;
use std::ffi::c_void;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::slice::from_raw_parts;
use std::sync::mpsc::{channel, Sender};
use std::thread;

/// The `Instance` provides safe APIs around the inherently unsafe access
/// to the underlying libcouchbase instance.
///
/// Its main purpose is to abstract all kinds of unsafe APIs and internally
/// runs it in its own thread to avoid synchronization. Requests and responses
/// are sent in and out through channels and queues.
///
/// An `Instance` is always bound to a bucket, since this is how lcb works.
pub struct Instance {
    sender: Sender<Box<InstanceRequest>>,
}

impl Instance {
    /// Creates a new `Instance` and runs it.
    pub fn new(connstr: &str, username: &str, password: &str) -> Result<Self, InstanceError> {
        let connstr = match CString::new(connstr) {
            Ok(c) => c,
            Err(_) => return Err(InstanceError::InvalidArgument),
        };
        let username = match CString::new(username) {
            Ok(c) => c,
            Err(_) => return Err(InstanceError::InvalidArgument),
        };
        let password = match CString::new(password) {
            Ok(c) => c,
            Err(_) => return Err(InstanceError::InvalidArgument),
        };

        let (tx, rx) = channel::<Box<InstanceRequest>>();

        let _handle = thread::Builder::new()
            .spawn(move || {
                let mut cropts = lcb_create_st {
                    version: 3,
                    v: unsafe { ::std::mem::zeroed() },
                };

                let mut instance: *mut lcb_INSTANCE = ptr::null_mut();

                unsafe {
                    cropts.v.v3.connstr = connstr.as_ptr();
                    cropts.v.v3.username = username.as_ptr();
                    cropts.v.v3.passwd = password.as_ptr();

                    if lcb_create(&mut instance, &cropts) != lcb_STATUS_LCB_SUCCESS {
                        // TODO: Log Err(InstanceError::CreateFailed);
                        return;
                    }

                    install_instance_callbacks(instance);

                    if lcb_connect(instance) != lcb_STATUS_LCB_SUCCESS {
                        // TODO: Log Err(InstanceError::ConnectFailed);
                        return;
                    }

                    if lcb_wait(instance) != lcb_STATUS_LCB_SUCCESS {
                        // TODO:  Err(InstanceError::WaitFailed);
                        return;
                    }

                    loop {
                        while let Ok(v) = rx.try_recv() {
                            v.encode(instance);
                        }
                        lcb_tick_nowait(instance);
                    }
                }
            })
            .expect("Could not create IO thread");

        Ok(Instance { sender: tx })
    }

    pub fn get(
        &self,
        id: String,
        options: Option<GetOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = ()> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(GetRequest::new(p, id, options)))
            .expect("Could not send get command into io loop");
        c.map_err(|_| ())
    }

    pub fn upsert(
        &self,
        id: String,
        content: Vec<u8>,
        flags: u32,
    ) -> impl Future<Item = MutationResult, Error = ()> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(UpsertRequest::new(p, id, content, flags)))
            .expect("Could not send upsert command into io loop");
        c.map_err(|_| ())
    }
}

#[derive(Debug)]
pub enum InstanceError {
    InvalidArgument,
    CreateFailed,
    ConnectFailed,
    WaitFailed,
    Other,
}

/// Installs the libcouchbase callbacks at the bucket level.
///
/// Since these callbacks go into the FFI layer they are by definition unsafe and
/// as a result put into their own method for easier auditing.
unsafe fn install_instance_callbacks(instance: *mut lcb_INSTANCE) {
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_GET as i32,
        Some(get_callback),
    );
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_STORE as i32,
        Some(store_callback),
    );
}

/// Holds the callback used for all get operations.
unsafe extern "C" fn get_callback(
    _instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    let get_res = res as *const lcb_RESPGET;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respget_cookie(get_res, &mut cookie_ptr);
    let sender = Box::from_raw(cookie_ptr as *mut oneshot::Sender<Option<GetResult>>);

    let status = lcb_respget_status(get_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        let mut flags: u32 = 0;
        let mut value_len: usize = 0;
        let mut value_ptr: *const c_char = ptr::null();
        lcb_respget_cas(get_res, &mut cas);
        lcb_respget_flags(get_res, &mut flags);
        lcb_respget_value(get_res, &mut value_ptr, &mut value_len);
        let value = from_raw_parts(value_ptr as *const u8, value_len);
        Some(GetResult::new(cas, value.to_vec(), flags))
    } else {
        // TODO: proper error handling and stuffs.
        None
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn store_callback(
    _instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    let store_res = res as *const lcb_RESPSTORE;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respstore_cookie(store_res, &mut cookie_ptr);
    let sender = Box::from_raw(cookie_ptr as *mut oneshot::Sender<MutationResult>);

    let mut cas: u64 = 0;
    lcb_respstore_cas(store_res, &mut cas);

    // TODO: ERROR HANDLING
    sender
        .send(MutationResult::new(cas))
        .expect("Could not complete Future!");
}
