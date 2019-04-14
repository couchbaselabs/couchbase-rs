use crate::options::*;
use crate::result::*;
use couchbase_sys::*;
use futures::sync::{mpsc, oneshot};
use std::ffi::{c_void, CString};
use std::os::raw::c_char;
use std::ptr;
use std::slice::from_raw_parts;

pub trait InstanceRequest: Send + 'static {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE);
}

#[derive(Debug)]
pub struct GetRequest {
    sender: oneshot::Sender<Option<GetResult>>,
    id: String,
    options: Option<GetOptions>,
}

impl GetRequest {
    pub fn new(
        sender: oneshot::Sender<Option<GetResult>>,
        id: String,
        options: Option<GetOptions>,
    ) -> Self {
        Self {
            sender,
            id,
            options,
        }
    }
}

impl InstanceRequest for GetRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");
        let mut command: *mut lcb_CMDGET = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;
        unsafe {
            lcb_cmdget_create(&mut command);
            lcb_cmdget_key(command, id_encoded.as_ptr(), id_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdget_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_get(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct UpsertRequest {
    sender: oneshot::Sender<MutationResult>,
    id: String,
    content: Vec<u8>,
    flags: u32,
    options: Option<UpsertOptions>,
}

impl UpsertRequest {
    pub fn new(
        sender: oneshot::Sender<MutationResult>,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<UpsertOptions>,
    ) -> Self {
        Self {
            sender,
            id,
            content,
            flags,
            options,
        }
    }
}

impl InstanceRequest for UpsertRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");

        let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;

        let value_len = self.content.len();
        let value = CString::new(self.content).expect("Could not turn value into lcb format");

        unsafe {
            lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
            lcb_cmdstore_key(command, id_encoded.as_ptr(), id_len);
            lcb_cmdstore_flags(command, self.flags);
            lcb_cmdstore_value(command, value.into_raw() as *const c_char, value_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdstore_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_store(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct InsertRequest {
    sender: oneshot::Sender<MutationResult>,
    id: String,
    content: Vec<u8>,
    flags: u32,
    options: Option<InsertOptions>,
}

impl InsertRequest {
    pub fn new(
        sender: oneshot::Sender<MutationResult>,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<InsertOptions>,
    ) -> Self {
        Self {
            sender,
            id,
            content,
            flags,
            options,
        }
    }
}

impl InstanceRequest for InsertRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");

        let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;

        let value_len = self.content.len();
        let value = CString::new(self.content).expect("Could not turn value into lcb format");

        unsafe {
            lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_ADD);
            lcb_cmdstore_key(command, id_encoded.as_ptr(), id_len);
            lcb_cmdstore_flags(command, self.flags);
            lcb_cmdstore_value(command, value.into_raw() as *const c_char, value_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdstore_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_store(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct ReplaceRequest {
    sender: oneshot::Sender<MutationResult>,
    id: String,
    content: Vec<u8>,
    flags: u32,
    options: Option<ReplaceOptions>,
}

impl ReplaceRequest {
    pub fn new(
        sender: oneshot::Sender<MutationResult>,
        id: String,
        content: Vec<u8>,
        flags: u32,
        options: Option<ReplaceOptions>,
    ) -> Self {
        Self {
            sender,
            id,
            content,
            flags,
            options,
        }
    }
}

impl InstanceRequest for ReplaceRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");

        let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;

        let value_len = self.content.len();
        let value = CString::new(self.content).expect("Could not turn value into lcb format");

        unsafe {
            lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_REPLACE);
            lcb_cmdstore_key(command, id_encoded.as_ptr(), id_len);
            lcb_cmdstore_flags(command, self.flags);
            lcb_cmdstore_value(command, value.into_raw() as *const c_char, value_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdstore_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_store(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct RemoveRequest {
    sender: oneshot::Sender<MutationResult>,
    id: String,
    options: Option<RemoveOptions>,
}

impl RemoveRequest {
    pub fn new(
        sender: oneshot::Sender<MutationResult>,
        id: String,
        options: Option<RemoveOptions>,
    ) -> Self {
        Self {
            sender,
            id,
            options,
        }
    }
}

impl InstanceRequest for RemoveRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let id_len = self.id.len();
        let id_encoded = CString::new(self.id).expect("Could not encode ID");
        let mut command: *mut lcb_CMDREMOVE = ptr::null_mut();

        let sender_boxed = Box::new(self.sender);
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;
        unsafe {
            lcb_cmdremove_create(&mut command);
            lcb_cmdremove_key(command, id_encoded.as_ptr(), id_len);
            if let Some(options) = self.options {
                if let Some(timeout) = options.timeout() {
                    lcb_cmdremove_timeout(command, timeout.as_millis() as u32);
                }
            }
            lcb_remove(instance, cookie, command);
        }
    }
}

#[derive(Debug)]
pub struct QueryRequest {
    sender: oneshot::Sender<QueryResult>,
    rows_sender: mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    meta_sender: oneshot::Sender<Vec<u8>>,
    meta_receiver: oneshot::Receiver<Vec<u8>>,
    statement: String,
    options: Option<QueryOptions>,
}

impl QueryRequest {
    pub fn new(
        sender: oneshot::Sender<QueryResult>,
        statement: String,
        options: Option<QueryOptions>,
    ) -> Self {
        let (meta_sender, meta_receiver) = oneshot::channel();
        let (rows_sender, rows_receiver) = mpsc::unbounded();
        Self {
            sender,
            rows_sender,
            rows_receiver,
            meta_sender,
            meta_receiver,
            statement,
            options,
        }
    }
}

impl InstanceRequest for QueryRequest {
    fn encode(self: Box<Self>, instance: *mut lcb_INSTANCE) {
        let statement_len = self.statement.len();
        let statement_encoded = CString::new(self.statement).expect("Could not encode Statement");
        let mut command: *mut lcb_CMDN1QL = ptr::null_mut();

        let sender_boxed = Box::new(QueryCookie {
            result: Some(self.sender),
            rows_sender: self.rows_sender,
            rows_receiver: Some(self.rows_receiver),
            meta_sender: self.meta_sender,
            meta_receiver: Some(self.meta_receiver),
        });
        let cookie = Box::into_raw(sender_boxed) as *mut c_void;
        unsafe {
            lcb_cmdn1ql_create(&mut command);
            lcb_cmdn1ql_statement(command, statement_encoded.as_ptr(), statement_len);
            lcb_cmdn1ql_callback(command, Some(n1ql_callback));
            lcb_n1ql(instance, cookie, command);
        }
    }
}

struct QueryCookie {
    result: Option<oneshot::Sender<QueryResult>>,
    rows_sender: mpsc::UnboundedSender<Vec<u8>>,
    rows_receiver: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    meta_sender: oneshot::Sender<Vec<u8>>,
    meta_receiver: Option<oneshot::Receiver<Vec<u8>>>,
}

unsafe extern "C" fn n1ql_callback(
    _instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPN1QL,
) {
    let mut row_len: usize = 0;
    let mut row_ptr: *const c_char = ptr::null();
    lcb_respn1ql_row(res, &mut row_ptr, &mut row_len);
    let row = from_raw_parts(row_ptr as *const u8, row_len);

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respn1ql_cookie(res, &mut cookie_ptr);
    let mut cookie = Box::from_raw(cookie_ptr as *mut QueryCookie);

    if cookie.result.is_some() {
        cookie
            .result
            .take()
            .expect("Could not take result!")
            .send(QueryResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
            ))
            .expect("Could not complete Future!");
    }

    if lcb_respn1ql_is_final(res) == 1 {
        cookie
            .meta_sender
            .send(row.to_vec())
            .expect("Could not send meta");
    } else {
        cookie
            .rows_sender
            .unbounded_send(row.to_vec())
            .expect("Could not send row");
        Box::into_raw(cookie);
    }
}
