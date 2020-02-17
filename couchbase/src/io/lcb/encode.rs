use crate::api::options::StoreSemantics;
use crate::api::{LookupInSpec, MutateInSpec};
use crate::io::lcb::callbacks::{analytics_callback, query_callback};
use crate::io::lcb::{AnalyticsCookie, QueryCookie};
use crate::io::request::*;

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

enum EncodedLookupSpec {
    Get { path_len: usize, path: CString },
    Exists { path_len: usize, path: CString },
    Count { path_len: usize, path: CString },
}

/// Encodes a `LookupInRequest` into its libcouchbase `lcb_CMDSUBDOC` representation.
pub fn encode_lookup_in(instance: *mut lcb_INSTANCE, request: LookupInRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

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
        lcb_subdocspecs_create(&mut specs, lookup_specs.len());

        let mut idx = 0;
        for lookup_spec in &lookup_specs {
            match lookup_spec {
                EncodedLookupSpec::Get { path_len, path } => {
                    lcb_subdocspecs_get(specs, idx, 0, path.as_ptr(), *path_len);
                }
                EncodedLookupSpec::Exists { path_len, path } => {
                    lcb_subdocspecs_exists(specs, idx, 0, path.as_ptr(), *path_len);
                }
                EncodedLookupSpec::Count { path_len, path } => {
                    lcb_subdocspecs_get_count(specs, idx, 0, path.as_ptr(), *path_len);
                }
            }
            idx = idx + 1;
        }

        lcb_cmdsubdoc_create(&mut command);
        lcb_cmdsubdoc_key(command, id.as_ptr(), id_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdsubdoc_timeout(command, timeout.as_micros() as u32);
        }
        if let Some(access_deleted) = request.options.access_deleted {
            lcb_cmdsubdoc_access_deleted(command, if access_deleted { 1 } else { 0 });
        }

        lcb_cmdsubdoc_specs(command, specs);
        lcb_subdoc(instance, cookie as *mut c_void, command);
        lcb_subdocspecs_destroy(specs);
        lcb_cmdsubdoc_destroy(command);
    }
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
pub fn encode_mutate_in(instance: *mut lcb_INSTANCE, request: MutateInRequest) {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));

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
        lcb_subdocspecs_create(&mut specs, mutate_specs.len());

        let mut idx = 0;
        for mutate_spec in &mutate_specs {
            match mutate_spec {
                EncodedMutateSpec::Insert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_dict_add(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::Upsert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_dict_upsert(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::Replace {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_replace(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::Remove { path_len, path } => {
                    lcb_subdocspecs_remove(specs, idx, 0, path.as_ptr(), *path_len);
                }
                EncodedMutateSpec::Counter {
                    path_len,
                    path,
                    delta,
                } => {
                    lcb_subdocspecs_counter(specs, idx, 0, path.as_ptr(), *path_len, *delta);
                }
                EncodedMutateSpec::ArrayAppend {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_array_add_last(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::ArrayPrepend {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_array_add_last(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::ArrayAddUnique {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_array_add_unique(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
                EncodedMutateSpec::ArrayInsert {
                    path_len,
                    path,
                    value_len,
                    value,
                } => {
                    lcb_subdocspecs_array_insert(
                        specs,
                        idx,
                        0,
                        path.as_ptr(),
                        *path_len,
                        value.as_ptr(),
                        *value_len,
                    );
                }
            }
            idx = idx + 1;
        }

        lcb_cmdsubdoc_create(&mut command);
        lcb_cmdsubdoc_key(command, id.as_ptr(), id_len);

        if let Some(timeout) = request.options.timeout {
            lcb_cmdsubdoc_timeout(command, timeout.as_micros() as u32);
        }
        if let Some(cas) = request.options.cas {
            lcb_cmdsubdoc_cas(command, cas);
        }
        if let Some(semantics) = request.options.store_semantics {
            let ss = match semantics {
                StoreSemantics::Replace => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_REPLACE,
                StoreSemantics::Upsert => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_UPSERT,
                StoreSemantics::Insert => lcb_SUBDOC_STORE_SEMANTICS_LCB_SUBDOC_STORE_INSERT,
            };
            lcb_cmdsubdoc_store_semantics(command, ss);
        }
        if let Some(expiry) = request.options.expiry {
            lcb_cmdsubdoc_expiry(command, expiry.as_micros() as u32);
        }
        if let Some(access_deleted) = request.options.access_deleted {
            lcb_cmdsubdoc_access_deleted(command, if access_deleted { 1 } else { 0 });
        }

        lcb_cmdsubdoc_specs(command, specs);
        lcb_subdoc(instance, cookie as *mut c_void, command);
        lcb_subdocspecs_destroy(specs);
        lcb_cmdsubdoc_destroy(command);
    }
}
