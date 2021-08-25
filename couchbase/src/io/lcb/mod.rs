mod callbacks;
mod encode;
mod instance;

use crate::api::error::CouchbaseResult;
use crate::api::results::{
    AnalyticsMetaData, AnalyticsResult, GenericManagementResult, QueryMetaData, QueryResult,
    SearchMetaData, SearchResult,
};

use encode::EncodeFailure;

use crate::io::request::Request;
use instance::{LcbInstance, LcbInstances};

use crate::{ViewMetaData, ViewResult, ViewRow};
use couchbase_sys::*;
use crossbeam_channel::RecvTimeoutError;
use crossbeam_channel::{unbounded, Receiver, Sender};
use log::{debug, warn};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{ptr, thread};

#[derive(Debug)]
pub struct IoCore {
    thread_handle: Option<JoinHandle<()>>,
    queue_tx: Sender<IoRequest>,
    connection_string: String,
    username: String,
    password: String,
}

impl IoCore {
    pub fn new(connection_string: String, username: String, password: String) -> Self {
        debug!("Using libcouchbase IO transport");

        let (queue_tx, queue_rx) = unbounded();

        let cstring = connection_string.clone();
        let uname = username.clone();
        let pwd = password.clone();
        let thread_handle = thread::spawn(move || run_lcb_loop(queue_rx, cstring, uname, pwd));
        Self {
            thread_handle: Some(thread_handle),
            queue_tx,
            connection_string,
            username,
            password,
        }
    }

    pub fn send(&self, request: Request) {
        self.queue_tx
            .send(IoRequest::Data(request))
            .expect("Could not send request")
    }

    pub fn open_bucket(&self, name: String) {
        self.queue_tx
            .send(IoRequest::OpenBucket {
                name,
                connection_string: self.connection_string.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
            })
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
    let mut instances = LcbInstances::default();

    match LcbInstance::new(
        connection_string.into_bytes(),
        username.into_bytes(),
        password.into_bytes(),
    ) {
        Ok(i) => instances.set_unbound(i),
        Err(e) => warn!("Could not open libcouchbase instance {}", e),
    };

    'running: loop {
        if instances.have_outstanding_requests() {
            while let Ok(req) = queue_rx.try_recv() {
                if instances.handle_request(req).unwrap() {
                    break 'running;
                }
            }
        } else {
            match queue_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(req) => {
                    if instances.handle_request(req).unwrap() {
                        // We got shut down, bail out.
                        break 'running;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    // The sender disconnected, bail out.
                    break 'running;
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Keep going, it will make sure to tick below and then block again
                }
            }
        }

        instances.tick_nowait().unwrap();
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
        ap: callbacks::VaList,
    ) -> c_int;
}

#[derive(Debug)]
pub enum IoRequest {
    Data(Request),
    OpenBucket {
        name: String,
        connection_string: String,
        username: String,
        password: String,
    },
    Shutdown,
}

fn encode_request(instance: *mut lcb_INSTANCE, request: Request) -> Result<(), EncodeFailure> {
    match request {
        Request::Get(r) => encode::encode_get(instance, r)?,
        Request::Query(r) => encode::encode_query(instance, r)?,
        Request::Analytics(r) => encode::encode_analytics(instance, r)?,
        Request::Search(r) => encode::encode_search(instance, r)?,
        Request::View(r) => encode::encode_view(instance, r)?,
        Request::Mutate(r) => encode::encode_mutate(instance, r)?,
        Request::Exists(r) => encode::encode_exists(instance, r)?,
        Request::Remove(r) => encode::encode_remove(instance, r)?,
        Request::LookupIn(r) => encode::encode_lookup_in(instance, r)?,
        Request::MutateIn(r) => encode::encode_mutate_in(instance, r)?,
        Request::GenericManagementRequest(r) => {
            encode::encode_generic_management_request(instance, r)?
        }
        Request::Ping(r) => encode::encode_ping(instance, r)?,
        Request::Counter(r) => encode::encode_counter(instance, r)?,
    }

    Ok(())
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

struct SearchCookie {
    sender: Option<futures::channel::oneshot::Sender<CouchbaseResult<SearchResult>>>,
    rows_sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: Option<futures::channel::mpsc::UnboundedReceiver<Vec<u8>>>,
    meta_sender: futures::channel::oneshot::Sender<SearchMetaData>,
    meta_receiver: Option<futures::channel::oneshot::Receiver<SearchMetaData>>,
    facet_sender: futures::channel::oneshot::Sender<serde_json::Value>,
    facet_receiver: Option<futures::channel::oneshot::Receiver<serde_json::Value>>,
}

struct ViewCookie {
    sender: Option<futures::channel::oneshot::Sender<CouchbaseResult<ViewResult>>>,
    rows_sender: futures::channel::mpsc::UnboundedSender<ViewRow>,
    rows_receiver: Option<futures::channel::mpsc::UnboundedReceiver<ViewRow>>,
    meta_sender: futures::channel::oneshot::Sender<ViewMetaData>,
    meta_receiver: Option<futures::channel::oneshot::Receiver<ViewMetaData>>,
}

/// This cookie can represent all different generic http requestes fired against lcb.
///
/// Note that we need an enum so we can match the correct request type on encode.
enum HttpCookie {
    GenericManagementRequest {
        sender: futures::channel::oneshot::Sender<CouchbaseResult<GenericManagementResult>>,
    },
}
