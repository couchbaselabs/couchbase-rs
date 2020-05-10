mod callbacks;
mod encode;
mod instance;

use crate::api::error::{CouchbaseResult, ErrorContext};
use crate::api::results::{AnalyticsMetaData, AnalyticsResult, QueryMetaData, QueryResult};
use crate::io::request::Request;
use instance::LcbInstance;

use callbacks::couchbase_error_from_lcb_status;

use couchbase_sys::*;
use crossbeam_channel::{unbounded, Receiver, Sender};
use log::debug;
use std::ffi::CStr;
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
    let mut instance = LcbInstance::new(
        connection_string.into_bytes(),
        username.into_bytes(),
        password.into_bytes(),
    )
    .unwrap();

    'running: loop {
        if instance.has_outstanding_requests() {
            while let Ok(req) = queue_rx.try_recv() {
                if instance.handle_request(req).unwrap() {
                    break 'running;
                }
            }
        } else if let Ok(req) = queue_rx.recv() {
            if instance.handle_request(req).unwrap() {
                break 'running;
            }
        }

        instance.tick_nowait().unwrap();
    }
}

/// Panicing is not good, but until we have better error handling at least
/// make it obvious that something is going wrong.
fn check_status_and_panic(status: lcb_STATUS, description: &'static str) {
    if status != lcb_STATUS_LCB_SUCCESS {
        panic!(
            "Operation lcb_{} failed with error: {}",
            description,
            couchbase_error_from_lcb_status(status, ErrorContext::default())
        )
    }
}

/// Helper method to ask the instance for the current bucket name and return it if any
/// is present.
///
/// Note that libcouchbase still wants to own the buffer, so we can only look into the
/// returned opaque str and clone it into ownership before return.
fn bucket_name_for_instance(instance: *mut lcb_INSTANCE) -> Option<String> {
    let mut bucket_ptr: *mut i8 = ptr::null_mut();
    let opaque_ptr = &mut bucket_ptr as *mut *mut i8;

    unsafe {
        let status = lcb_cntl(
            instance,
            LCB_CNTL_GET as i32,
            LCB_CNTL_BUCKETNAME as i32,
            opaque_ptr as *mut c_void,
        );
        if status == lcb_STATUS_LCB_SUCCESS && !bucket_ptr.is_null() {
            Some(CStr::from_ptr(bucket_ptr).to_str().unwrap().into())
        } else {
            None
        }
    }
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
pub enum IoRequest {
    Data(Request),
    OpenBucket { name: String },
    Shutdown,
}

fn encode_request(instance: *mut lcb_INSTANCE, request: Request) {
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
