use couchbase_sys::*;
use log::debug;
use std::ffi::{CStr};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::slice::from_raw_parts;
use std::{ptr};
use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::results::{GetResult, QueryResult, MutationResult};
use crate::api::MutationToken;

use crate::io::lcb::{QueryCookie, decrement_outstanding_requests, wrapped_vsnprintf};

pub unsafe extern "C" fn store_callback(
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

pub unsafe extern "C" fn get_callback(
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

pub unsafe extern "C" fn query_callback(
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

const LOG_MSG_LENGTH: usize = 1024;

pub unsafe extern "C" fn logger_callback(
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

pub unsafe extern "C" fn open_callback(_instance: *mut lcb_INSTANCE, err: lcb_STATUS) {
    debug!("Completed bucket open attempt (status: 0x{:x})", &err);
}