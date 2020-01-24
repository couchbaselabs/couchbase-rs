use crate::api::options::QueryScanConsistency;
use crate::io::lcb::QueryCookie;
use crate::io::request::{GetRequest, QueryRequest, UpsertRequest};

use crate::io::lcb::callbacks::query_callback;

use couchbase_sys::*;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;

/// Helper method to turn a string into a tuple of CString and its length.
#[inline]
fn into_cstring<T: Into<Vec<u8>>>(input: T) -> (usize, CString) {
    let input = input.into();
    (
        input.len(),
        CString::new(input).expect("Could not encode into CString"),
    )
}

/// Encodes a `GetRequest` into its libcouchbase `lcb_CMDGET` representation.
pub fn encode_get(instance: *mut lcb_INSTANCE, request: GetRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDGET = ptr::null_mut();
    unsafe {
        lcb_cmdget_create(&mut command);
        lcb_cmdget_key(command, id.as_ptr(), id_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdget_timeout(command, timeout.as_micros() as u32);
        }

        lcb_get(instance, cookie as *mut c_void, command);
        lcb_cmdget_destroy(command);
    }
}

/// Encodes a `UpsertRequest` into its libcouchbase `lcb_CMDSTORE` representation.
pub fn encode_upsert(instance: *mut lcb_INSTANCE, request: UpsertRequest) {
    let (id_len, id) = into_cstring(request.id);
    let (value_len, value) = into_cstring(request.content);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDSTORE = ptr::null_mut();
    unsafe {
        lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
        lcb_cmdstore_key(command, id.as_ptr(), id_len);
        lcb_cmdstore_value(command, value.as_ptr(), value_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
        }

        lcb_store(instance, cookie as *mut c_void, command);
        lcb_cmdstore_destroy(command);
    }
}

/// Encodes a `QueryRequest` into its libcouchbase `lcb_CMDQUERY` representation.
pub fn encode_query(instance: *mut lcb_INSTANCE, request: QueryRequest) {
    let (statement_len, statement) = into_cstring(request.statement);

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let cookie = Box::into_raw(Box::new(QueryCookie {
        sender: Some(request.sender),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
    }));

    let mut command: *mut lcb_CMDQUERY = ptr::null_mut();
    unsafe {
        lcb_cmdquery_create(&mut command);
        lcb_cmdquery_statement(command, statement.as_ptr(), statement_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdquery_timeout(command, timeout.as_micros() as u32);
        }
        if let Some(sc) = request.options.scan_consistency {
            match sc {
                QueryScanConsistency::RequestPlus => {
                    lcb_cmdquery_consistency(
                        command,
                        lcb_QUERY_CONSISTENCY_LCB_QUERY_CONSISTENCY_REQUEST,
                    );
                }
                _ => {}
            }
        }

        lcb_cmdquery_callback(command, Some(query_callback));
        lcb_query(instance, cookie as *mut c_void, command);
        lcb_cmdquery_destroy(command);
    }
}
