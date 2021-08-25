use crate::api::{LookupInSpec, MutateInSpec};
use crate::io::lcb::callbacks::{
    analytics_callback, query_callback, search_callback, view_callback,
};
use crate::io::lcb::{AnalyticsCookie, HttpCookie, QueryCookie, SearchCookie, ViewCookie};
use crate::io::request::*;
use crate::{api::options::StoreSemantics, CouchbaseResult, ErrorContext};
use futures::channel::oneshot::Sender;
use log::{debug, warn};
use serde_json::Value;

use couchbase_sys::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;
use uuid::Uuid;

use super::callbacks::couchbase_error_from_lcb_status;

#[derive(Debug)]
pub struct EncodeFailure(lcb_STATUS);

/// Helper method to turn a string into a tuple of CString and its length.
#[inline]
pub fn into_cstring<T: Into<Vec<u8>>>(input: T) -> (usize, CString) {
    let input = input.into();
    (
        input.len(),
        CString::new(input).expect("Could not encode into CString"),
    )
}

/// Verifies the libcouchbase return status code and fails the original request.
fn verify<T>(
    status: lcb_STATUS,
    sender: *mut Sender<CouchbaseResult<T>>,
) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        if let Err(_) = sender.send(Err(err)) {
            debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
        }
        return Err(EncodeFailure(status));
    }
    Ok(())
}

fn verify_query(status: lcb_STATUS, sender: *mut QueryCookie) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let mut sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        if let Err(_) = sender.sender.take().unwrap().send(Err(err)) {
            debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
        }
        // Close the rest that needs to be closed
        sender.rows_sender.close_channel();
        return Err(EncodeFailure(status));
    }
    Ok(())
}

fn verify_analytics(status: lcb_STATUS, sender: *mut AnalyticsCookie) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let mut sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        if let Err(_) = sender.sender.take().unwrap().send(Err(err)) {
            debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
        }
        // Close the rest that needs to be closed
        sender.rows_sender.close_channel();
        return Err(EncodeFailure(status));
    }
    Ok(())
}

fn verify_search(status: lcb_STATUS, sender: *mut SearchCookie) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let mut sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        if let Err(_) = sender.sender.take().unwrap().send(Err(err)) {
            debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
        }
        // Close the rest that needs to be closed
        sender.rows_sender.close_channel();
        return Err(EncodeFailure(status));
    }
    Ok(())
}

fn verify_view(status: lcb_STATUS, sender: *mut ViewCookie) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let mut sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        if let Err(_) = sender.sender.take().unwrap().send(Err(err)) {
            debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
        }
        // Close the rest that needs to be closed
        sender.rows_sender.close_channel();
        return Err(EncodeFailure(status));
    }
    Ok(())
}

fn verify_http(status: lcb_STATUS, sender: *mut HttpCookie) -> Result<(), EncodeFailure> {
    if status != lcb_STATUS_LCB_SUCCESS {
        if sender.is_null() {
            warn!("Failed to notify request of encode failure because the pointer is null. This is a bug!");
            return Ok(());
        }
        let sender = unsafe { Box::from_raw(sender) };
        let mut ctx = ErrorContext::default();
        if let Ok(msg) = unsafe { CStr::from_ptr(lcb_strerror_short(status)) }.to_str() {
            ctx.insert("msg", Value::String(msg.to_string()));
        }
        let err = couchbase_error_from_lcb_status(status, ctx);
        match *sender {
            HttpCookie::GenericManagementRequest { sender } => {
                if let Err(_) = sender.send(Err(err)) {
                    debug!("Failed to notify request of encode failure, because the listener has been already dropped.");
                }
            }
        }

        return Err(EncodeFailure(status));
    }
    Ok(())
}

/// Encodes a `GetRequest` into its libcouchbase `lcb_CMDGET` representation.
///
/// Note that this method also handles get_and_lock and get_and_touch by looking
/// at the ty (type) enum of the get request. If one of them is used their inner
/// duration is passed down to libcouchbase either as a locktime or the expiry.
pub fn encode_get(instance: *mut lcb_INSTANCE, request: GetRequest) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDGET = ptr::null_mut();
    unsafe {
        verify(lcb_cmdget_create(&mut command), cookie)?;
        verify(lcb_cmdget_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdget_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        match request.ty {
            GetRequestType::Get { options } => {
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdget_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
            }
            GetRequestType::GetAndLock { lock_time, options } => {
                verify(
                    lcb_cmdget_locktime(command, lock_time.as_micros() as u32),
                    cookie,
                )?;

                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdget_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
            }
            GetRequestType::GetAndTouch { expiry, options } => {
                verify(
                    lcb_cmdget_expiry(command, expiry.as_micros() as u32),
                    cookie,
                )?;

                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdget_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
            }
        };

        verify(lcb_get(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdget_destroy(command), cookie)?;
    }
    Ok(())
}

/// Encodes a `ExistsRequest` into its libcouchbase `lcb_CMDEXISTS` representation.
pub fn encode_exists(
    instance: *mut lcb_INSTANCE,
    request: ExistsRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDEXISTS = ptr::null_mut();
    unsafe {
        verify(lcb_cmdexists_create(&mut command), cookie)?;
        verify(lcb_cmdexists_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdexists_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        if let Some(timeout) = request.options.timeout {
            verify(
                lcb_cmdexists_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        verify(lcb_exists(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdexists_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `MutateRequest` into its libcouchbase `lcb_CMDSTORE` representation.
///
/// This method covers insert, upsert and replace since they are very similar and
/// only differ on certain properties.
pub fn encode_mutate(
    instance: *mut lcb_INSTANCE,
    request: MutateRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let (value_len, value) = into_cstring(request.content);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDSTORE = ptr::null_mut();
    unsafe {
        match request.ty {
            MutateRequestType::Upsert { options } => {
                verify(
                    lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT),
                    cookie,
                )?;
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdstore_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
                if let Some(expiry) = options.expiry {
                    verify(
                        lcb_cmdstore_expiry(command, expiry.as_secs() as u32),
                        cookie,
                    )?;
                }
            }
            MutateRequestType::Insert { options } => {
                verify(
                    lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_INSERT),
                    cookie,
                )?;
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdstore_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
                if let Some(expiry) = options.expiry {
                    verify(
                        lcb_cmdstore_expiry(command, expiry.as_secs() as u32),
                        cookie,
                    )?;
                }
            }
            MutateRequestType::Replace { options } => {
                verify(
                    lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_REPLACE),
                    cookie,
                )?;
                if let Some(cas) = options.cas {
                    verify(lcb_cmdstore_cas(command, cas), cookie)?;
                }
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdstore_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
                if let Some(expiry) = options.expiry {
                    verify(
                        lcb_cmdstore_expiry(command, expiry.as_secs() as u32),
                        cookie,
                    )?;
                }
            }
            MutateRequestType::Append { options } => {
                verify(
                    lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_APPEND),
                    cookie,
                )?;
                if let Some(cas) = options.cas {
                    verify(lcb_cmdstore_cas(command, cas), cookie)?;
                }
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdstore_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
            }
            MutateRequestType::Prepend { options } => {
                verify(
                    lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_PREPEND),
                    cookie,
                )?;
                if let Some(cas) = options.cas {
                    verify(lcb_cmdstore_cas(command, cas), cookie)?;
                }
                if let Some(timeout) = options.timeout {
                    verify(
                        lcb_cmdstore_timeout(command, timeout.as_micros() as u32),
                        cookie,
                    )?;
                }
            }
        }
        verify(lcb_cmdstore_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdstore_value(command, value.as_ptr(), value_len),
            cookie,
        )?;
        verify(
            lcb_cmdstore_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        verify(lcb_store(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdstore_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `RemoveRequest` into its libcouchbase `lcb_CMDREMOVE` representation.
pub fn encode_remove(
    instance: *mut lcb_INSTANCE,
    request: RemoveRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDREMOVE = ptr::null_mut();
    unsafe {
        verify(lcb_cmdremove_create(&mut command), cookie)?;
        verify(lcb_cmdremove_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdremove_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        if let Some(cas) = request.options.cas {
            verify(lcb_cmdremove_cas(command, cas), cookie)?;
        }
        if let Some(timeout) = request.options.timeout {
            verify(
                lcb_cmdremove_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        verify(lcb_remove(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdremove_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `CounterRequest` into its libcouchbase `lcb_CMDCOUNTER` representation.
///
/// This method covers increment and decrement since they are effectively the same operation but
/// with a different operation applied to the delta value.
pub fn encode_counter(
    instance: *mut lcb_INSTANCE,
    request: CounterRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDCOUNTER = ptr::null_mut();
    unsafe {
        verify(lcb_cmdcounter_create(&mut command), cookie)?;
        verify(lcb_cmdcounter_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdcounter_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        if let Some(cas) = request.options.cas {
            verify(lcb_cmdcounter_cas(command, cas), cookie)?;
        }
        if let Some(timeout) = request.options.timeout {
            verify(
                lcb_cmdcounter_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }
        if let Some(expiry) = request.options.expiry {
            verify(
                lcb_cmdcounter_expiry(command, expiry.as_secs() as u32),
                cookie,
            )?;
        }

        verify(lcb_cmdcounter_delta(command, request.options.delta), cookie)?;
        verify(
            lcb_counter(instance, cookie as *mut c_void, command),
            cookie,
        )?;
        verify(lcb_cmdcounter_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `QueryRequest` into its libcouchbase `lcb_CMDQUERY` representation.
pub fn encode_query(
    instance: *mut lcb_INSTANCE,
    mut request: QueryRequest,
) -> Result<(), EncodeFailure> {
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
        verify_query(lcb_cmdquery_create(&mut command), cookie)?;
        verify_query(
            lcb_cmdquery_payload(command, payload.as_ptr(), payload_len),
            cookie,
        )?;

        if let Some(a) = request.options.adhoc {
            verify_query(lcb_cmdquery_adhoc(command, a.into()), cookie)?;
        }

        if let Some(s) = request.scope {
            let (scope_len, scope) = into_cstring(s);
            verify_query(
                lcb_cmdquery_scope_name(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
        }

        verify_query(lcb_cmdquery_callback(command, Some(query_callback)), cookie)?;
        verify_query(lcb_query(instance, cookie as *mut c_void, command), cookie)?;
        verify_query(lcb_cmdquery_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `AnalyticsRequest` into its libcouchbase `lcb_CMDANALYTICS` representation.
pub fn encode_analytics(
    instance: *mut lcb_INSTANCE,
    mut request: AnalyticsRequest,
) -> Result<(), EncodeFailure> {
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
        verify_analytics(lcb_cmdanalytics_create(&mut command), cookie)?;
        verify_analytics(
            lcb_cmdanalytics_payload(command, payload.as_ptr(), payload_len),
            cookie,
        )?;
        verify_analytics(
            lcb_cmdanalytics_callback(command, Some(analytics_callback)),
            cookie,
        )?;
        if let Some(s) = request.scope {
            let (scope_len, scope) = into_cstring(s);
            verify_analytics(
                lcb_cmdanalytics_scope_name(command, scope.as_ptr(), scope_len),
                cookie,
            )?;
        }
        verify_analytics(
            lcb_analytics(instance, cookie as *mut c_void, command),
            cookie,
        )?;
        verify_analytics(lcb_cmdanalytics_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `SearchRequest` into its libcouchbase `lcb_CMDSEARCH` representation.
pub fn encode_search(
    instance: *mut lcb_INSTANCE,
    mut request: SearchRequest,
) -> Result<(), EncodeFailure> {
    request.options.index = Some(request.index);
    request.options.query = Some(request.query);

    let (payload_len, payload) = into_cstring(serde_json::to_vec(&request.options).unwrap());

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let (facet_sender, facet_receiver) = futures::channel::oneshot::channel();
    let cookie = Box::into_raw(Box::new(SearchCookie {
        sender: Some(request.sender),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
        facet_sender,
        facet_receiver: Some(facet_receiver),
    }));

    let mut command: *mut lcb_CMDSEARCH = ptr::null_mut();
    unsafe {
        verify_search(lcb_cmdsearch_create(&mut command), cookie)?;
        verify_search(
            lcb_cmdsearch_payload(command, payload.as_ptr(), payload_len),
            cookie,
        )?;
        verify_search(
            lcb_cmdsearch_callback(command, Some(search_callback)),
            cookie,
        )?;
        verify_search(lcb_search(instance, cookie as *mut c_void, command), cookie)?;
        verify_search(lcb_cmdsearch_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `ViewRequest` into its libcouchbase `lcb_CMDVIEW` representation.
pub fn encode_view(instance: *mut lcb_INSTANCE, request: ViewRequest) -> Result<(), EncodeFailure> {
    let (ddoc_name_len, ddoc_name) = into_cstring(request.design_document);
    let (view_name_len, view_name) = into_cstring(request.view_name);
    let (payload_len, payload) = into_cstring(request.options);

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let cookie = Box::into_raw(Box::new(ViewCookie {
        sender: Some(request.sender),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
    }));

    let mut command: *mut lcb_CMDVIEW = ptr::null_mut();
    unsafe {
        verify_view(lcb_cmdview_create(&mut command), cookie)?;
        verify_view(
            lcb_cmdview_design_document(command, ddoc_name.as_ptr(), ddoc_name_len),
            cookie,
        )?;
        verify_view(
            lcb_cmdview_view_name(command, view_name.as_ptr(), view_name_len),
            cookie,
        )?;
        verify_view(
            lcb_cmdview_option_string(command, payload.as_ptr(), payload_len),
            cookie,
        )?;
        verify_view(lcb_cmdview_callback(command, Some(view_callback)), cookie)?;
        verify_view(lcb_view(instance, cookie as *mut c_void, command), cookie)?;
        verify_view(lcb_cmdview_destroy(command), cookie)?;
    }

    Ok(())
}

enum EncodedLookupSpec {
    Get { path_len: usize, path: CString },
    Exists { path_len: usize, path: CString },
    Count { path_len: usize, path: CString },
}

/// Encodes a `LookupInRequest` into its libcouchbase `lcb_CMDSUBDOC` representation.
pub fn encode_lookup_in(
    instance: *mut lcb_INSTANCE,
    request: LookupInRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let lookup_specs = request
        .specs
        .into_iter()
        .map(|spec| match spec {
            LookupInSpec::Get { path } => {
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Get { path_len, path }
            }
            LookupInSpec::Exists { path } => {
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Exists { path_len, path }
            }
            LookupInSpec::Count { path } => {
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Count { path_len, path }
            }
        })
        .collect::<Vec<_>>();

    let mut command: *mut lcb_CMDSUBDOC = ptr::null_mut();
    let mut specs: *mut lcb_SUBDOCSPECS = ptr::null_mut();
    unsafe {
        verify(
            lcb_subdocspecs_create(&mut specs, lookup_specs.len()),
            cookie,
        )?;

        let mut idx = 0;
        for lookup_spec in &lookup_specs {
            match lookup_spec {
                EncodedLookupSpec::Get { path_len, path } => {
                    verify(
                        lcb_subdocspecs_get(specs, idx, 0, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedLookupSpec::Exists { path_len, path } => {
                    verify(
                        lcb_subdocspecs_exists(specs, idx, 0, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedLookupSpec::Count { path_len, path } => {
                    verify(
                        lcb_subdocspecs_get_count(specs, idx, 0, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
            }
            idx += 1;
        }

        verify(lcb_cmdsubdoc_create(&mut command), cookie)?;
        verify(lcb_cmdsubdoc_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdsubdoc_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        if let Some(timeout) = request.options.timeout {
            verify(
                lcb_cmdsubdoc_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }
        if let Some(access_deleted) = request.options.access_deleted {
            verify(
                lcb_cmdsubdoc_access_deleted(command, if access_deleted { 1 } else { 0 }),
                cookie,
            )?;
        }

        verify(lcb_cmdsubdoc_specs(command, specs), cookie)?;
        verify(lcb_subdoc(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_subdocspecs_destroy(specs), cookie)?;
        verify(lcb_cmdsubdoc_destroy(command), cookie)?;
    }

    Ok(())
}

pub enum EncodedMutateSpec {
    Replace {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    Insert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    Upsert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    ArrayAddUnique {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    Remove {
        path_len: usize,
        path: CString,
    },
    Counter {
        path_len: usize,
        path: CString,
        delta: i64,
    },
    ArrayAppend {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    ArrayPrepend {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
    ArrayInsert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
    },
}

/// Encodes a `MutateInRequest` into its libcouchbase `lcb_CMDSUBDOC` representation.
pub fn encode_mutate_in(
    instance: *mut lcb_INSTANCE,
    request: MutateInRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mutate_specs = request
        .specs
        .into_iter()
        .map(|spec| match spec {
            MutateInSpec::Replace { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Replace {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::Insert { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Insert {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::Upsert { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Upsert {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::ArrayAddUnique { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayAddUnique {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::Remove { path } => {
                let (path_len, path) = into_cstring(path);
                EncodedMutateSpec::Remove { path_len, path }
            }
            MutateInSpec::Counter { path, delta } => {
                let (path_len, path) = into_cstring(path);
                EncodedMutateSpec::Counter {
                    path_len,
                    path,
                    delta,
                }
            }
            MutateInSpec::ArrayAppend { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayAppend {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::ArrayPrepend { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayPrepend {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
            MutateInSpec::ArrayInsert { path, value } => {
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayInsert {
                    path_len,
                    path,
                    value_len,
                    value,
                }
            }
        })
        .collect::<Vec<_>>();

    let mut command: *mut lcb_CMDSUBDOC = ptr::null_mut();
    let mut specs: *mut lcb_SUBDOCSPECS = ptr::null_mut();
    unsafe {
        verify(
            lcb_subdocspecs_create(&mut specs, mutate_specs.len()),
            cookie,
        )?;

        let mut idx = 0;
        for mutate_spec in &mutate_specs {
            match mutate_spec {
                EncodedMutateSpec::Insert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_dict_add(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Upsert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_dict_upsert(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Replace {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_replace(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Remove { path_len, path } => {
                    verify(
                        lcb_subdocspecs_remove(specs, idx, 0, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Counter {
                    path_len,
                    path,
                    delta,
                } => {
                    verify(
                        lcb_subdocspecs_counter(specs, idx, 0, path.as_ptr(), *path_len, *delta),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::ArrayAppend {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_last(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::ArrayPrepend {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_last(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::ArrayAddUnique {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_unique(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::ArrayInsert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    verify(
                        lcb_subdocspecs_array_insert(
                            specs,
                            idx,
                            0,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
            }
            idx += 1;
        }

        verify(lcb_cmdsubdoc_create(&mut command), cookie)?;
        verify(lcb_cmdsubdoc_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdsubdoc_collection(
                command,
                scope.as_ptr(),
                scope_len,
                collection.as_ptr(),
                collection_len,
            ),
            cookie,
        )?;

        if let Some(timeout) = request.options.timeout {
            verify(
                lcb_cmdsubdoc_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }
        if let Some(cas) = request.options.cas {
            verify(lcb_cmdsubdoc_cas(command, cas), cookie)?;
        }
        if let Some(semantics) = request.options.store_semantics {
            let ss = match semantics {
                StoreSemantics::Replace => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_REPLACE,
                StoreSemantics::Upsert => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_UPSERT,
                StoreSemantics::Insert => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_INSERT,
            };
            verify(lcb_cmdsubdoc_store_semantics(command, ss), cookie)?;
        }
        if let Some(expiry) = request.options.expiry {
            verify(
                lcb_cmdsubdoc_expiry(command, expiry.as_micros() as u32),
                cookie,
            )?;
        }
        if let Some(access_deleted) = request.options.access_deleted {
            verify(
                lcb_cmdsubdoc_access_deleted(command, if access_deleted { 1 } else { 0 }),
                cookie,
            )?;
        }

        verify(lcb_cmdsubdoc_specs(command, specs), cookie)?;
        verify(lcb_subdoc(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_subdocspecs_destroy(specs), cookie)?;
        verify(lcb_cmdsubdoc_destroy(command), cookie)?;
    }

    Ok(())
}

pub fn encode_generic_management_request(
    instance: *mut lcb_INSTANCE,
    request: GenericManagementRequest,
) -> Result<(), EncodeFailure> {
    let (path_len, path) = into_cstring(request.path);
    let cookie = Box::into_raw(Box::new(HttpCookie::GenericManagementRequest {
        sender: request.sender,
    }));

    let (body_len, body) = into_cstring(request.payload.unwrap_or(String::from("")));
    let (content_type_len, content_type) =
        into_cstring(request.content_type.unwrap_or(String::from("")));

    let mut command: *mut lcb_CMDHTTP = ptr::null_mut();
    unsafe {
        verify_http(
            lcb_cmdhttp_create(&mut command, lcb_HTTP_TYPE_LCB_HTTP_TYPE_MANAGEMENT),
            cookie,
        )?;
        let method = match request.method.as_str() {
            "get" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_GET,
            "put" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_PUT,
            "post" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_POST,
            "delete" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_DELETE,
            _ => panic!("Unknown HTTP method used"),
        };
        verify_http(lcb_cmdhttp_method(command, method), cookie)?;
        verify_http(lcb_cmdhttp_path(command, path.as_ptr(), path_len), cookie)?;

        if let Some(timeout) = request.timeout {
            verify_http(
                lcb_cmdhttp_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        if content_type_len > 0 {
            verify_http(
                lcb_cmdhttp_content_type(command, content_type.as_ptr(), content_type_len),
                cookie,
            )?;
        }

        if body_len > 0 {
            verify_http(lcb_cmdhttp_body(command, body.as_ptr(), body_len), cookie)?;
        }

        verify_http(lcb_http(instance, cookie as *mut c_void, command), cookie)?;
        verify_http(lcb_cmdhttp_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes a `PingRequest` into its libcouchbase `lcb_CMDPING` representation.
pub fn encode_ping(instance: *mut lcb_INSTANCE, request: PingRequest) -> Result<(), EncodeFailure> {
    let cookie = Box::into_raw(Box::new(request.sender));

    let report_id = request
        .options
        .report_id
        .unwrap_or(Uuid::new_v4().to_hyphenated().to_string());
    let (report_id_len, c_report_id) = into_cstring(report_id);

    let mut command: *mut lcb_CMDPING = ptr::null_mut();
    unsafe {
        verify(lcb_cmdping_create(&mut command), cookie)?;
        verify(
            lcb_cmdping_report_id(command, c_report_id.as_ptr(), report_id_len),
            cookie,
        )?;
        verify(lcb_cmdping_all(command), cookie)?;
        verify(lcb_ping(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdping_destroy(command), cookie)?;
    }

    Ok(())
}
