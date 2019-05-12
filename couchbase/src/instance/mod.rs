//! A safe wrapper around the unsafe libcouchbase instance.
//!
//! Warning: you'll find lots of unsafe blocks in this file, but hopefully less
//! of them in the other files as as a result. If something segfaults, this is
//! likely the place to look.

mod request;

use crate::error::CouchbaseError;
use crate::options::*;
use crate::result::*;
use crate::subdoc::*;
use couchbase_sys::*;
use futures::sync::oneshot;
use futures::Future;
use request::*;
use std::cell::RefCell;
use std::ffi::c_void;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint};
use std::ptr;
use std::slice::from_raw_parts;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time::Duration;

const LOG_MSG_LENGTH: usize = 1024;

/// Keeps track of per-instance state.
#[derive(Debug)]
struct InstanceCookie {
    outstanding: usize,
    shutdown: bool,
}

impl InstanceCookie {
    pub fn new() -> Self {
        InstanceCookie {
            outstanding: 0,
            shutdown: false,
        }
    }

    pub fn increment_outstanding(&mut self) {
        self.outstanding += 1
    }

    pub fn decrement_outstanding(&mut self) {
        self.outstanding -= 1
    }

    pub fn has_outstanding(&self) -> bool {
        self.outstanding > 0
    }

    pub fn shutdown(&self) -> bool {
        self.shutdown
    }

    pub fn set_shutdown(&mut self) {
        self.shutdown = true;
    }
}

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
    handle: RefCell<Option<thread::JoinHandle<()>>>,
}

impl Instance {
    /// Creates a new `Instance` and runs it.
    pub fn new(connstr: &str, username: &str, password: &str) -> Result<Self, CouchbaseError> {
        let connstr = match CString::new(connstr) {
            Ok(c) => c,
            Err(_) => return Err(CouchbaseError::InvalidValue),
        };
        let username = match CString::new(username) {
            Ok(c) => c,
            Err(_) => return Err(CouchbaseError::InvalidValue),
        };
        let password = match CString::new(password) {
            Ok(c) => c,
            Err(_) => return Err(CouchbaseError::InvalidValue),
        };

        let (tx, rx) = channel::<Box<InstanceRequest>>();

        let handle = thread::Builder::new()
            .spawn(move || {
                let mut cropts = lcb_create_st {
                    version: 4,
                    v: unsafe { ::std::mem::zeroed() },
                };

                let mut instance: *mut lcb_INSTANCE = ptr::null_mut();

                let mut logger = lcb_logprocs_st {
                    version: 0,
                    v: unsafe { ::std::mem::zeroed() },
                };

                unsafe {
                    logger.v.v0.callback = Some(logging_callback);

                    cropts.v.v4.connstr = connstr.as_ptr();
                    cropts.v.v4.username = username.as_ptr();
                    cropts.v.v4.passwd = password.as_ptr();
                    cropts.v.v4.logger = &mut logger;

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

                    let mut instance_cookie = Box::new(InstanceCookie::new());
                    lcb_set_cookie(
                        instance,
                        &instance_cookie as *const Box<InstanceCookie> as *const c_void,
                    );

                    loop {
                        if instance_cookie.has_outstanding() {
                            while let Ok(v) = rx.try_recv() {
                                v.encode(instance);
                                instance_cookie.increment_outstanding();
                            }
                        } else if let Ok(v) = rx.recv() {
                            v.encode(instance);
                            instance_cookie.increment_outstanding();
                        }

                        if instance_cookie.shutdown() {
                            break;
                        }

                        lcb_tick_nowait(instance);
                    }

                    // instance cookie is in scope and will be dropped automatically
                    lcb_destroy(instance);
                }
            })
            .expect("Could not create IO thread");

        Ok(Instance {
            sender: tx,
            handle: RefCell::new(Some(handle)),
        })
    }

    pub fn shutdown(&self) -> Result<(), CouchbaseError> {
        match self.handle.borrow_mut().take() {
            Some(h) => {
                match self.sender.send(Box::new(ShutdownRequest::new())) {
                    Ok(_) => (),
                    Err(_e) => return Err(CouchbaseError::FutureError),
                };
                h.join()
                    .expect("failed while waiting for io thread to join");
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn get(
        &self,
        id: String,
        options: Option<GetOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(GetRequest::new(p, id, options)))
            .expect("Could not send get command into io loop");
        map_oneshot_error(c)
    }

    pub fn get_and_lock(
        &self,
        id: String,
        options: Option<GetAndLockOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(GetAndLockRequest::new(p, id, options)))
            .expect("Could not send getAndLock command into io loop");
        map_oneshot_error(c)
    }

    pub fn get_and_touch(
        &self,
        id: String,
        expiration: Duration,
        options: Option<GetAndTouchOptions>,
    ) -> impl Future<Item = Option<GetResult>, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(GetAndTouchRequest::new(
                p, id, expiration, options,
            )))
            .expect("Could not send getAndTouch command into io loop");
        map_oneshot_error(c)
    }

    pub fn exists(
        &self,
        id: String,
        options: Option<ExistsOptions>,
    ) -> impl Future<Item = Option<ExistsResult>, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(ExistsRequest::new(p, id, options)))
            .expect("Could not send exists command into io loop");
        map_oneshot_error(c)
    }

    pub fn upsert(
        &self,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<UpsertOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(UpsertRequest::new(p, id, content, flags, options)))
            .expect("Could not send upsert command into io loop");
        map_oneshot_error(c)
    }

    pub fn insert(
        &self,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<InsertOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(InsertRequest::new(p, id, content, flags, options)))
            .expect("Could not send insert command into io loop");
        map_oneshot_error(c)
    }

    pub fn replace(
        &self,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<ReplaceOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(ReplaceRequest::new(
                p, id, content, flags, options,
            )))
            .expect("Could not send replace command into io loop");
        map_oneshot_error(c)
    }

    pub fn remove(
        &self,
        id: String,
        options: Option<RemoveOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(RemoveRequest::new(p, id, options)))
            .expect("Could not send remove command into io loop");
        map_oneshot_error(c)
    }

    pub fn touch(
        &self,
        id: String,
        expiration: Duration,
        options: Option<TouchOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(TouchRequest::new(p, id, expiration, options)))
            .expect("Could not send touch command into io loop");
        map_oneshot_error(c)
    }

    pub fn unlock(
        &self,
        id: String,
        cas: u64,
        options: Option<UnlockOptions>,
    ) -> impl Future<Item = MutationResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(UnlockRequest::new(p, id, cas, options)))
            .expect("Could not send unlock command into io loop");
        map_oneshot_error(c)
    }

    pub fn lookup_in(
        &self,
        id: String,
        specs: Vec<LookupInSpec>,
        options: Option<LookupInOptions>,
    ) -> impl Future<Item = Option<LookupInResult>, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(LookupInRequest::new(p, id, specs, options)))
            .expect("Could not send lookupIn command into io loop");
        map_oneshot_error(c)
    }

    pub fn query(
        &self,
        statement: String,
        options: Option<QueryOptions>,
    ) -> impl Future<Item = QueryResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(QueryRequest::new(p, statement, options)))
            .expect("Could not send query command into io loop");
        map_oneshot_error(c)
    }

    pub fn analytics_query(
        &self,
        statement: String,
        options: Option<AnalyticsOptions>,
    ) -> impl Future<Item = AnalyticsResult, Error = CouchbaseError> {
        let (p, c) = oneshot::channel();
        self.sender
            .send(Box::new(AnalyticsRequest::new(p, statement, options)))
            .expect("Could not send analytics query command into io loop");
        map_oneshot_error(c)
    }
}

fn map_oneshot_error<T>(
    receiver: oneshot::Receiver<Result<T, CouchbaseError>>,
) -> impl Future<Item = T, Error = CouchbaseError> {
    receiver.then(|value| match value {
        Ok(v) => match v {
            Ok(i) => Ok(i),
            Err(e) => Err(e),
        },
        Err(_) => Err(CouchbaseError::FutureError),
    })
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
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_REMOVE as i32,
        Some(remove_callback),
    );
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_TOUCH as i32,
        Some(touch_callback),
    );
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_UNLOCK as i32,
        Some(unlock_callback),
    );
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_EXISTS as i32,
        Some(exists_callback),
    );
    lcb_install_callback3(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_SDLOOKUP as i32,
        Some(lookup_in_callback),
    );
}

/// Helper method to grab the instance cookiea and decrement the outstanding requests.
unsafe fn decrement_outstanding_requests(instance: *mut lcb_INSTANCE) {
    let instance_cookie_ptr: *const c_void = lcb_get_cookie(instance);
    let mut instance_cookie = Box::from_raw(instance_cookie_ptr as *mut Box<InstanceCookie>);
    instance_cookie.decrement_outstanding();
    Box::into_raw(instance_cookie);
}

/// Holds the callback used for all get operations.
unsafe extern "C" fn get_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let get_res = res as *const lcb_RESPGET;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respget_cookie(get_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut oneshot::Sender<Result<Option<GetResult>, CouchbaseError>>,
    );

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
        Ok(Some(GetResult::new(cas, value.to_vec(), flags)))
    } else if status == lcb_STATUS_LCB_KEY_ENOENT {
        Ok(None)
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn store_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let store_res = res as *const lcb_RESPSTORE;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respstore_cookie(store_res, &mut cookie_ptr);
    let sender =
        Box::from_raw(cookie_ptr as *mut oneshot::Sender<Result<MutationResult, CouchbaseError>>);

    let mut cas: u64 = 0;
    lcb_respstore_cas(store_res, &mut cas);

    let status = lcb_respstore_status(store_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        Ok(MutationResult::new(cas))
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn remove_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let remove_res = res as *const lcb_RESPREMOVE;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respremove_cookie(remove_res, &mut cookie_ptr);
    let sender =
        Box::from_raw(cookie_ptr as *mut oneshot::Sender<Result<MutationResult, CouchbaseError>>);

    let mut cas: u64 = 0;
    lcb_respremove_cas(remove_res, &mut cas);

    let status = lcb_respremove_status(remove_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        Ok(MutationResult::new(cas))
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn touch_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let touch_res = res as *const lcb_RESPTOUCH;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_resptouch_cookie(touch_res, &mut cookie_ptr);
    let sender =
        Box::from_raw(cookie_ptr as *mut oneshot::Sender<Result<MutationResult, CouchbaseError>>);

    let mut cas: u64 = 0;
    lcb_resptouch_cas(touch_res, &mut cas);

    let status = lcb_resptouch_status(touch_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        Ok(MutationResult::new(cas))
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn unlock_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let unlock_res = res as *const lcb_RESPUNLOCK;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respunlock_cookie(unlock_res, &mut cookie_ptr);
    let sender =
        Box::from_raw(cookie_ptr as *mut oneshot::Sender<Result<MutationResult, CouchbaseError>>);

    let mut cas: u64 = 0;
    lcb_respunlock_cas(unlock_res, &mut cas);

    let status = lcb_respunlock_status(unlock_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        Ok(MutationResult::new(cas))
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn exists_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let exists_res = res as *const lcb_RESPEXISTS;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respexists_cookie(exists_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut oneshot::Sender<Result<Option<ExistsResult>, CouchbaseError>>,
    );

    let mut cas: u64 = 0;
    lcb_respexists_cas(exists_res, &mut cas);

    let status = lcb_respexists_status(exists_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        if lcb_respexists_is_found(exists_res) != 0 {
            Ok(Some(ExistsResult::new(cas)))
        } else {
            Ok(None)
        }
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn lookup_in_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let lookup_res = res as *const lcb_RESPSUBDOC;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respsubdoc_cookie(lookup_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut oneshot::Sender<Result<Option<LookupInResult>, CouchbaseError>>,
    );

    let status = lcb_respsubdoc_status(lookup_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_respsubdoc_cas(lookup_res, &mut cas);

        let num_fields = lcb_respsubdoc_result_size(lookup_res);
        let mut fields = Vec::with_capacity(num_fields);
        for idx in 0..num_fields {
            let mut value_len: usize = 0;
            let mut value_ptr: *const c_char = ptr::null();
            lcb_respsubdoc_result_value(lookup_res, idx, &mut value_ptr, &mut value_len);
            let value = from_raw_parts(value_ptr as *const u8, value_len);
            let field_status = lcb_respsubdoc_result_status(lookup_res, idx);
            fields.push(LookupInField::new(
                CouchbaseError::from(field_status),
                value.to_vec(),
            ));
        }

        Ok(Some(LookupInResult::new(cas, fields)))
    } else if status == lcb_STATUS_LCB_KEY_ENOENT {
        Ok(None)
    } else {
        Err(CouchbaseError::from(status))
    };
    sender.send(result).expect("Could not complete Future!");
}

unsafe extern "C" fn logging_callback(
    _procs: *mut lcb_logprocs_st,
    _iid: c_uint,
    _subsys: *const c_char,
    severity: c_int,
    _srcfile: *const c_char,
    _srcline: c_int,
    fmt: *const c_char,
    ap: *mut __va_list_tag,
) {
    let level = match severity {
        0 => log::Level::Trace,
        1 => log::Level::Debug,
        2 => log::Level::Info,
        3 => log::Level::Warn,
        _ => log::Level::Error,
    };

    let mut target_buffer = [0u8; LOG_MSG_LENGTH];
    let result = wrapped_vsnprintf(&mut target_buffer[0] as *mut u8 as *mut i8, LOG_MSG_LENGTH as c_uint, fmt, ap) as usize;
    let decoded = CStr::from_bytes_with_nul(&target_buffer[0..=result]).unwrap();

    log::log!(level, "{}", decoded.to_str().unwrap());
}

extern "C" { 
    /// Wrapper function defined in `utils.c` to wrap vsnprintf for logging purposes.
    fn wrapped_vsnprintf(buf: *mut c_char, size: c_uint, format: *const c_char, ap: *mut __va_list_tag) -> c_int;
}