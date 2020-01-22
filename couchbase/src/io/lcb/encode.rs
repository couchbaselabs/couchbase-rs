use crate::api::options::QueryScanConsistency;
use crate::io::lcb::QueryCookie;
use crate::io::request::{GetRequest, QueryRequest, UpsertRequest};

use crate::io::lcb::callbacks::query_callback;

use couchbase_sys::*;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::ptr;

pub fn encode_get(instance: *mut lcb_INSTANCE, request: GetRequest) {
    let id_len = request.id().len();
    let id_encoded = CString::new(request.id().clone()).expect("Could not encode ID");
    let mut command: *mut lcb_CMDGET = ptr::null_mut();

    let timeout = request.options().timeout.map(|t| t.as_micros() as u32);
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

pub fn encode_upsert(instance: *mut lcb_INSTANCE, request: UpsertRequest) {
    let id_len = request.id().len();
    let id_encoded = CString::new(request.id().clone()).expect("Could not encode ID");
    let mut command: *mut lcb_CMDSTORE = ptr::null_mut();

    let value_len = request.content().len();
    let value = CString::new(request.content()).expect("Could not turn value into lcb format");

    let timeout = request.options().timeout.map(|t| t.as_micros() as u32);
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

pub fn encode_query(instance: *mut lcb_INSTANCE, request: QueryRequest) {
    let statement_len = request.statement().len();
    let statement_encoded =
        CString::new(request.statement().clone()).expect("Could not encode Statement");
    let mut command: *mut lcb_CMDQUERY = ptr::null_mut();

    let timeout = request.options().timeout.map(|t| t.as_micros() as u32);
    let scan_consistency = request
        .options()
        .scan_consistency
        .as_ref()
        .map(|s| match s {
            QueryScanConsistency::NotBounded => lcb_QUERY_CONSISTENCY_LCB_QUERY_CONSISTENCY_NONE,
            QueryScanConsistency::RequestPlus => {
                lcb_QUERY_CONSISTENCY_LCB_QUERY_CONSISTENCY_REQUEST
            }
        });

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
        if let Some(sc) = scan_consistency {
            lcb_cmdquery_consistency(command, sc);
        }
        lcb_cmdquery_callback(command, Some(query_callback));
        lcb_query(instance, cookie, command);
        lcb_cmdquery_destroy(command);
    }
}
