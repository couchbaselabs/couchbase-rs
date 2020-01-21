use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::results::{GetResult, QueryMetaData, QueryResult, MutationResult};
use crate::api::options::{QueryScanConsistency};
use crate::io::request::{UpsertRequest, GetRequest, QueryRequest, Request};
use crate::api::MutationToken;

use couchbase_sys::*;
use log::debug;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::slice::from_raw_parts;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;
use std::{ptr, thread};

pub struct IoCore {
    _thread_handle: JoinHandle<()>,
    queue_tx: Sender<IoRequest>,
}

impl IoCore {
    pub fn new(connection_string: String, username: String, password: String) -> Self {
        debug!("Using libcouchbase IO transport");

        let (queue_tx, queue_rx) = channel();
        let thread_handle =
            thread::spawn(move || run_lcb_loop(queue_rx, connection_string, username, password));
        Self {
            _thread_handle: thread_handle,
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
        lcb_create(&mut instance, create_options);
        lcb_createopts_destroy(create_options);

        install_instance_callbacks(instance);

        lcb_connect(instance);
        lcb_wait(instance, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);

        lcb_set_cookie(
            instance,
            &instance_cookie as *const Box<InstanceCookie> as *const c_void,
        );
    }

    'run: loop {
        if instance_cookie.has_outstanding() {
            while let Ok(req) = queue_rx.try_recv() {
                if handle_io_request(req, instance, &mut instance_cookie) {
                    break 'run;
                }
            }
        } else {
            while let Ok(req) = queue_rx.recv() {
                if handle_io_request(req, instance, &mut instance_cookie) {
                    break 'run;
                } else {
                    // We now might have a request outstanding so we cannot
                    // go back to blocking recv and need to switch to try_recv
                    break;
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
    instance_cookie: &mut Box<InstanceCookie>,
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
            let name = CString::new(name).unwrap();
            lcb_open(instance, name.as_ptr(), name_len);
            lcb_wait(instance, lcb_WAITFLAGS_LCB_WAIT_DEFAULT);
        },
    }
    return false;
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
    lcb_set_open_callback(instance, Some(open_callback));
}

unsafe fn decrement_outstanding_requests(instance: *mut lcb_INSTANCE) {
    let instance_cookie_ptr: *const c_void = lcb_get_cookie(instance);
    let mut instance_cookie = Box::from_raw(instance_cookie_ptr as *mut Box<InstanceCookie>);
    instance_cookie.decrement_outstanding();
    Box::into_raw(instance_cookie);
}

unsafe extern "C" fn open_callback(_instance: *mut lcb_INSTANCE, err: lcb_STATUS) {
    debug!("Completed bucket open attempt (status: 0x{:x})", &err);
}

const LOG_MSG_LENGTH: usize = 1024;

unsafe extern "C" fn logger_callback(
    _procs: *const lcb_LOGGER,
    _iid: u64,
    _subsys: *const c_char,
    severity: lcb_LOG_SEVERITY,
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
    let result = wrapped_vsnprintf(
        &mut target_buffer[0] as *mut u8 as *mut i8,
        LOG_MSG_LENGTH as c_uint,
        fmt,
        ap,
    ) as usize;
    let decoded = CStr::from_bytes_with_nul(&target_buffer[0..=result]).unwrap();

    log::log!(level, "{}", decoded.to_str().unwrap());
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
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<GetResult>>,
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
        Ok(GetResult::new(value.to_vec(), cas, flags))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            ErrorContext::default(),
        ))
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
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<MutationResult>>,
    );

    let status = lcb_respstore_status(store_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_respstore_cas(store_res, &mut cas);

        let mut lcb_mutation_token = lcb_MUTATION_TOKEN { uuid_: 0, seqno_: 0, vbid_: 0 };
        lcb_respstore_mutation_token(store_res, &mut lcb_mutation_token);
        let mutation_token = if lcb_mutation_token.uuid_ != 0 {
            Some(MutationToken::new(lcb_mutation_token.uuid_, lcb_mutation_token.seqno_, lcb_mutation_token.vbid_))
        } else {
            None
        };
        Ok(MutationResult::new(cas, mutation_token))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            ErrorContext::default(),
        ))
    };
    sender.send(result).expect("Could not complete Future!");
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
        Request::Get(r) => encode_get(instance, r),
        Request::Query(r) => encode_query(instance, r),
        Request::Upsert(r) => encode_upsert(instance, r),
    }
}

fn encode_get(instance: *mut lcb_INSTANCE, request: GetRequest) {
    let id_len = request.id().len();
    let id_encoded = CString::new(request.id().clone()).expect("Could not encode ID");
    let mut command: *mut lcb_CMDGET = ptr::null_mut();

    let timeout = request.options().timeout().map(|t| t.as_micros() as u32);
    let sender_boxed = Box::new(request.sender());
    let cookie = Box::into_raw(sender_boxed) as *mut c_void;
    unsafe {
        lcb_cmdget_create(&mut command);
        lcb_cmdget_key(command, id_encoded.as_ptr(), id_len);
        if let Some(timeout) = timeout {
            lcb_cmdget_timeout(command, timeout);
        }
        lcb_get(instance, cookie, command);
        lcb_cmdget_destroy(command);
    }
}

fn encode_upsert(instance: *mut lcb_INSTANCE, request: UpsertRequest) {
    let id_len = request.id().len();
    let id_encoded = CString::new(request.id().clone()).expect("Could not encode ID");
    let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

    let value_len = request.content().len();
    let value = CString::new(request.content()).expect("Could not turn value into lcb format");

    let timeout = request.options().timeout().map(|t| t.as_micros() as u32);
    let sender_boxed = Box::new(request.sender());
    let cookie = Box::into_raw(sender_boxed) as *mut c_void;

    unsafe {
        lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
        lcb_cmdstore_key(command, id_encoded.as_ptr(), id_len);
        lcb_cmdstore_value(command, value.into_raw() as *const c_char, value_len);
        if let Some(timeout) = timeout {
            lcb_cmdstore_timeout(command, timeout);
        }
        lcb_store(instance, cookie, command);
        lcb_cmdstore_destroy(command);
    }
}

struct QueryCookie {
    sender: Option<futures::channel::oneshot::Sender<CouchbaseResult<QueryResult>>>,
    rows_sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: Option<futures::channel::mpsc::UnboundedReceiver<Vec<u8>>>,
    meta_sender: futures::channel::oneshot::Sender<QueryMetaData>,
    meta_receiver: Option<futures::channel::oneshot::Receiver<QueryMetaData>>,
}

fn encode_query(instance: *mut lcb_INSTANCE, request: QueryRequest) {
    let statement_len = request.statement().len();
    let statement_encoded =
        CString::new(request.statement().clone()).expect("Could not encode Statement");
    let mut command: *mut lcb_CMDQUERY = ptr::null_mut();

    let timeout = request.options().timeout().map(|t| t.as_micros() as u32);
    let scan_consistency = match request.options().scan_consistency {
        QueryScanConsistency::NotBounded => lcb_QUERY_CONSISTENCY_LCB_QUERY_CONSISTENCY_NONE,
        QueryScanConsistency::RequestPlus => lcb_QUERY_CONSISTENCY_LCB_QUERY_CONSISTENCY_REQUEST,
    };

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let sender_boxed = Box::new(QueryCookie {
        sender: Some(request.sender()),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
    });
    let cookie = Box::into_raw(sender_boxed) as *mut c_void;
    unsafe {
        lcb_cmdquery_create(&mut command);
        lcb_cmdquery_statement(command, statement_encoded.as_ptr(), statement_len);
        if let Some(timeout) = timeout {
            lcb_cmdquery_timeout(command, timeout);
        }
        lcb_cmdquery_consistency(command, scan_consistency);
        lcb_cmdquery_callback(command, Some(query_callback));
        lcb_query(instance, cookie, command);
        lcb_cmdquery_destroy(command);
    }
}

unsafe extern "C" fn query_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPQUERY,
) {
    let mut row_len: usize = 0;
    let mut row_ptr: *const c_char = ptr::null();
    lcb_respquery_row(res, &mut row_ptr, &mut row_len);
    let row = from_raw_parts(row_ptr as *const u8, row_len);

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respquery_cookie(res, &mut cookie_ptr);
    let mut cookie = Box::from_raw(cookie_ptr as *mut QueryCookie);

    if cookie.sender.is_some() {
        cookie
            .sender
            .take()
            .expect("Could not take result!")
            .send(Ok(QueryResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
            )))
            .expect("Could not complete query future");
    }

    if lcb_respquery_is_final(res) != 0 {
        cookie.rows_sender.close_channel();
        cookie.meta_sender.send(serde_json::from_slice(row).unwrap()).expect("Could not send meta");
        decrement_outstanding_requests(instance);
    } else {
        cookie.rows_sender.unbounded_send(row.to_vec()).expect("Could not send rows");
        Box::into_raw(cookie);
    }
}

#[allow(non_upper_case_globals)]
fn couchbase_error_from_lcb_status(status: lcb_STATUS, ctx: ErrorContext) -> CouchbaseError {
    match status {
        lcb_STATUS_LCB_ERR_DOCUMENT_NOT_FOUND => CouchbaseError::DocumentNotFound { ctx },
        _ => CouchbaseError::Generic { ctx },
    }
}
