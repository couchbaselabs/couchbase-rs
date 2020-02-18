mod callbacks;
mod encode;

use crate::api::error::CouchbaseResult;
use crate::api::results::{AnalyticsMetaData, AnalyticsResult, QueryMetaData, QueryResult};
use crate::io::request::Request;

use callbacks::{
    exists_callback, get_callback, logger_callback, lookup_in_callback, mutate_in_callback,
    open_callback, remove_callback, store_callback,
};

use couchbase_sys::*;
use crossbeam_channel::{unbounded, Receiver, Sender};
use log::debug;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::thread::JoinHandle;
use std::{ptr, thread};

pub struct IoCore {
    thread_handle: Option<JoinHandle<()>>,
    queue_tx: Sender<IoRequest>,
}

impl IoCore {
    pub fn new(connection_string: String, username: String, password: String) -> Self {
        debug!("Using libcouchbase IO transport");

        let (queue_tx, queue_rx) = unbounded();
        let thread_handle =
            thread::spawn(move || run_lcb_loop(queue_rx, connection_string, username, password));
        Self {
            thread_handle: Some(thread_handle),
            queue_tx,
        }
    }

    pub fn send(&self, request: Request) {
        self.queue_tx
            .send(IoRequest::Data(request))
            .expect("Could not send request")
    }

    pub fn open_bucket(&self, name: String) {
        self.queue_tx
            .send(IoRequest::OpenBucket { name })
            .expect("Could not send open bucket request")
    }
}

impl Drop for IoCore {
    fn drop(&mut self) {
        debug!("Dropping LCB IoCore, sending shutdown signal");
        self.queue_tx
            .send(IoRequest::Shutdown)
            .expect("Failure while shutting down!");
        self.thread_handle
            .take()
            .unwrap()
            .join()
            .expect("Failure while waiting for lcb thread to die!");
        debug!("LCB Thread completed, finishing Drop sequence");
    }
}

fn run_lcb_loop(
    queue_rx: Receiver<IoRequest>,
    connection_string: String,
    username: String,
    password: String,
) {
    let connection_string_len = connection_string.len();
    let connection_string = CString::new(connection_string).unwrap();

    let username_len = username.len();
    let username = CString::new(username).unwrap();
    let password_len = password.len();
    let password = CString::new(password).unwrap();

    let mut instance: *mut lcb_INSTANCE = ptr::null_mut();
    let mut create_options: *mut lcb_CREATEOPTS = ptr::null_mut();
    let mut instance_cookie = Box::new(InstanceCookie::new());
    let mut logger: *mut lcb_LOGGER = ptr::null_mut();

    unsafe {
        lcb_createopts_create(&mut create_options, lcb_INSTANCE_TYPE_LCB_TYPE_CLUSTER);

        lcb_logger_create(&mut logger, ptr::null_mut());
        lcb_logger_callback(logger, Some(logger_callback));
        lcb_createopts_logger(create_options, logger);

        lcb_createopts_connstr(
            create_options,
            connection_string.as_ptr(),
            connection_string_len,
        );
        lcb_createopts_credentials(
            create_options,
            username.as_ptr(),
            username_len,
            password.as_ptr(),
            password_len,
        );
        debug!(
            "Result of create: {:x}",
            lcb_create(&mut instance, create_options)
        );
        lcb_createopts_destroy(create_options);

        install_instance_callbacks(instance);

        lcb_connect(instance);
        lcb_wait(instance, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);

        lcb_set_cookie(
            instance,
            &instance_cookie as *const Box<InstanceCookie> as *const c_void,
        );
    }

    'running: loop {
        if instance_cookie.has_outstanding() {
            while let Ok(req) = queue_rx.try_recv() {
                if handle_io_request(req, instance, &mut instance_cookie) {
                    break 'running;
                }
            }
        } else {
            if let Ok(req) = queue_rx.recv() {
                if handle_io_request(req, instance, &mut instance_cookie) {
                    break 'running;
                }
            }
        }

        unsafe {
            lcb_tick_nowait(instance);
        }
    }

    unsafe {
        lcb_wait(instance, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);
        lcb_destroy(instance);
    }
}

fn handle_io_request(
    req: IoRequest,
    instance: *mut lcb_INSTANCE,
    instance_cookie: &mut InstanceCookie,
) -> bool {
    match req {
        IoRequest::Data(r) => {
            encode(instance, r);
            instance_cookie.increment_outstanding();
        }
        IoRequest::Shutdown => return true,
        IoRequest::OpenBucket { name } => unsafe {
            debug!("Starting bucket open for {}", &name);
            let name_len = name.len();
            let c_name = CString::new(name.clone()).unwrap();
            lcb_open(instance, c_name.as_ptr(), name_len);
            lcb_wait(instance, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);
            debug!("Finished bucket open for {}", &name);
        },
    };
    false
}

unsafe fn install_instance_callbacks(instance: *mut lcb_INSTANCE) {
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_GET as i32,
        Some(get_callback),
    );
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_STORE as i32,
        Some(store_callback),
    );
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_EXISTS as i32,
        Some(exists_callback),
    );
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_REMOVE as i32,
        Some(remove_callback),
    );
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_SDMUTATE as i32,
        Some(mutate_in_callback),
    );
    lcb_install_callback(
        instance,
        lcb_CALLBACK_TYPE_LCB_CALLBACK_SDLOOKUP as i32,
        Some(lookup_in_callback),
    );
    lcb_set_open_callback(instance, Some(open_callback));
}

unsafe fn decrement_outstanding_requests(instance: *mut lcb_INSTANCE) {
    let instance_cookie_ptr: *const c_void = lcb_get_cookie(instance);
    let mut instance_cookie = Box::from_raw(instance_cookie_ptr as *mut Box<InstanceCookie>);
    instance_cookie.decrement_outstanding();
    Box::into_raw(instance_cookie);
}

/// Helper method to ask the instance for the current bucket name and return it if any
/// is present.
fn bucket_name_for_instance(instance: *mut lcb_INSTANCE) -> Option<String> {
    let mut bucket_ptr = ptr::null_mut();
    let raw_ptr = &mut bucket_ptr as *mut *mut i8;

    unsafe {
        let status = lcb_cntl(
            instance,
            LCB_CNTL_GET as i32,
            LCB_CNTL_BUCKETNAME as i32,
            raw_ptr as *mut c_void,
        );
        if status == lcb_STATUS_LCB_SUCCESS {
            return Some(CStr::from_ptr(bucket_ptr).to_str().unwrap().into());
        } else {
            return None;
        }
    };
}

extern "C" {
    /// Wrapper function defined in `utils.c` to wrap vsnprintf for logging purposes.
    fn wrapped_vsnprintf(
        buf: *mut c_char,
        size: c_uint,
        format: *const c_char,
        ap: *mut __va_list_tag,
    ) -> c_int;
}

#[derive(Debug)]
enum IoRequest {
    Data(Request),
    OpenBucket { name: String },
    Shutdown,
}

#[derive(Debug)]
struct InstanceCookie {
    outstanding: usize,
}

impl InstanceCookie {
    pub fn new() -> Self {
        Self { outstanding: 0 }
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
}

fn encode(instance: *mut lcb_INSTANCE, request: Request) {
    match request {
        Request::Get(r) => encode::encode_get(instance, r),
        Request::Query(r) => encode::encode_query(instance, r),
        Request::Analytics(r) => encode::encode_analytics(instance, r),
        Request::Mutate(r) => encode::encode_mutate(instance, r),
        Request::Exists(r) => encode::encode_exists(instance, r),
        Request::Remove(r) => encode::encode_remove(instance, r),
        Request::LookupIn(r) => encode::encode_lookup_in(instance, r),
        Request::MutateIn(r) => encode::encode_mutate_in(instance, r),
    }
}

struct QueryCookie {
    sender: Option<futures::channel::oneshot::Sender<CouchbaseResult<QueryResult>>>,
    rows_sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: Option<futures::channel::mpsc::UnboundedReceiver<Vec<u8>>>,
    meta_sender: futures::channel::oneshot::Sender<QueryMetaData>,
    meta_receiver: Option<futures::channel::oneshot::Receiver<QueryMetaData>>,
}

struct AnalyticsCookie {
    sender: Option<futures::channel::oneshot::Sender<CouchbaseResult<AnalyticsResult>>>,
    rows_sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: Option<futures::channel::mpsc::UnboundedReceiver<Vec<u8>>>,
    meta_sender: futures::channel::oneshot::Sender<AnalyticsMetaData>,
    meta_receiver: Option<futures::channel::oneshot::Receiver<AnalyticsMetaData>>,
}
