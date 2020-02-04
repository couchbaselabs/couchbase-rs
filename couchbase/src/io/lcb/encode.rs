use crate::io::lcb::{AnalyticsCookie, QueryCookie};
use crate::io::request::*;

use crate::io::lcb::callbacks::{analytics_callback, query_callback};

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
///
/// Note that this method also handles get_and_lock and get_and_touch by looking
/// at the ty (type) enum of the get request. If one of them is used their inner
/// duration is passed down to libcouchbase either as a locktime or the expiry.
pub fn encode_get(instance: *mut lcb_INSTANCE, request: GetRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDGET = ptr::null_mut();
    unsafe {
        lcb_cmdget_create(&mut command);
        lcb_cmdget_key(command, id.as_ptr(), id_len);

        match request.ty {
            GetRequestType::Get { options } => {
                if let Some(timeout) = options.timeout {
                    lcb_cmdget_timeout(command, timeout.as_micros() as u32);
                }
            }
            GetRequestType::GetAndLock { lock_time, options } => {
                lcb_cmdget_locktime(command, lock_time.as_micros() as u32);

                if let Some(timeout) = options.timeout {
                    lcb_cmdget_timeout(command, timeout.as_micros() as u32);
                }
            }
            GetRequestType::GetAndTouch { expiry, options } => {
                lcb_cmdget_expiry(command, expiry.as_micros() as u32);

                if let Some(timeout) = options.timeout {
                    lcb_cmdget_timeout(command, timeout.as_micros() as u32);
                }
            }
        };

        lcb_get(instance, cookie as *mut c_void, command);
        lcb_cmdget_destroy(command);
    }
}

/// Encodes a `ExistsRequest` into its libcouchbase `lcb_CMDEXISTS` representation.
pub fn encode_exists(instance: *mut lcb_INSTANCE, request: ExistsRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDEXISTS = ptr::null_mut();
    unsafe {
        lcb_cmdexists_create(&mut command);
        lcb_cmdexists_key(command, id.as_ptr(), id_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdexists_timeout(command, timeout.as_micros() as u32);
        }

        lcb_exists(instance, cookie as *mut c_void, command);
        lcb_cmdexists_destroy(command);
    }
}

/// Encodes a `MutateRequest` into its libcouchbase `lcb_CMDSTORE` representation.
///
/// This method covers insert, upsert and replace since they are very similar and
/// only differ on certain properties.
pub fn encode_mutate(instance: *mut lcb_INSTANCE, request: MutateRequest) {
    let (id_len, id) = into_cstring(request.id);
    let (value_len, value) = into_cstring(request.content);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDSTORE = ptr::null_mut();
    unsafe {
        lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
        lcb_cmdstore_key(command, id.as_ptr(), id_len);
        lcb_cmdstore_value(command, value.as_ptr(), value_len);

        match request.ty {
            MutateRequestType::Upsert { options } => {
                if let Some(timeout) = options.timeout {
                    lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
                }
                if let Some(expiry) = options.expiry {
                    lcb_cmdstore_expiry(command, expiry.as_secs() as u32);
                }
            }
            MutateRequestType::Insert { options } => {
                if let Some(timeout) = options.timeout {
                    lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
                }
                if let Some(expiry) = options.expiry {
                    lcb_cmdstore_expiry(command, expiry.as_secs() as u32);
                }
            }
            MutateRequestType::Replace { options } => {
                if let Some(cas) = options.cas {
                    lcb_cmdstore_cas(command, cas);
                }
                if let Some(timeout) = options.timeout {
                    lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
                }
                if let Some(expiry) = options.expiry {
                    lcb_cmdstore_expiry(command, expiry.as_secs() as u32);
                }
            }
        }

        lcb_store(instance, cookie as *mut c_void, command);
        lcb_cmdstore_destroy(command);
    }
}

/// Encodes a `RemoveRequest` into its libcouchbase `lcb_CMDREMOVE` representation.
pub fn encode_remove(instance: *mut lcb_INSTANCE, request: RemoveRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

    let mut command: *mut lcb_CMDREMOVE = ptr::null_mut();
    unsafe {
        lcb_cmdremove_create(&mut command);
        lcb_cmdremove_key(command, id.as_ptr(), id_len);

        if let Some(cas) = request.options.cas {
            lcb_cmdremove_cas(command, cas);
        }
        if let Some(timeout) = request.options.timeout {
            lcb_cmdremove_timeout(command, timeout.as_micros() as u32);
        }

        lcb_remove(instance, cookie as *mut c_void, command);
        lcb_cmdremove_destroy(command);
    }
}

/// Encodes a `QueryRequest` into its libcouchbase `lcb_CMDQUERY` representation.
pub fn encode_query(instance: *mut lcb_INSTANCE, mut request: QueryRequest) {
    request.options.statement = Some(request.statement);
    let (payload_len, payload) = into_cstring(serde_json::to_vec(&request.options).unwrap());

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
        lcb_cmdquery_payload(command, payload.as_ptr(), payload_len);

        if let Some(a) = request.options.adhoc {
            lcb_cmdquery_adhoc(command, a.into());
        }

        lcb_cmdquery_callback(command, Some(query_callback));
        lcb_query(instance, cookie as *mut c_void, command);
        lcb_cmdquery_destroy(command);
    }
}

/// Encodes a `AnalyticsRequest` into its libcouchbase `lcb_CMDANALYTICS` representation.
pub fn encode_analytics(instance: *mut lcb_INSTANCE, mut request: AnalyticsRequest) {
    request.options.statement = Some(request.statement);
    let (payload_len, payload) = into_cstring(serde_json::to_vec(&request.options).unwrap());

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let cookie = Box::into_raw(Box::new(AnalyticsCookie {
        sender: Some(request.sender),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
    }));

    let mut command: *mut lcb_CMDANALYTICS = ptr::null_mut();
    unsafe {
        lcb_cmdanalytics_create(&mut command);
        lcb_cmdanalytics_payload(command, payload.as_ptr(), payload_len);
        lcb_cmdanalytics_callback(command, Some(analytics_callback));
        lcb_analytics(instance, cookie as *mut c_void, command);
        lcb_cmdanalytics_destroy(command);
    }
}
