use crate::api::options::StoreSemantics;
use crate::api::{LookupInSpec, MutateInSpec};
use crate::io::lcb::callbacks::{analytics_callback, query_callback, search_callback};
use crate::io::lcb::{AnalyticsCookie, HttpCookie, QueryCookie, SearchCookie};
use crate::io::request::*;

use couchbase_sys::*;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use uuid::Uuid;

/// Helper method to turn a string into a tuple of CString and its length.
#[inline]
pub fn into_cstring<T: Into<Vec<u8>>>(input: T) -> (usize, CString) {
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
        match request.ty {
            MutateRequestType::Upsert { options } => {
                lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_UPSERT);
                if let Some(timeout) = options.timeout {
                    lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
                }
                if let Some(expiry) = options.expiry {
                    lcb_cmdstore_expiry(command, expiry.as_secs() as u32);
                }
            }
            MutateRequestType::Insert { options } => {
                lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_INSERT);
                if let Some(timeout) = options.timeout {
                    lcb_cmdstore_timeout(command, timeout.as_micros() as u32);
                }
                if let Some(expiry) = options.expiry {
                    lcb_cmdstore_expiry(command, expiry.as_secs() as u32);
                }
            }
            MutateRequestType::Replace { options } => {
                lcb_cmdstore_create(&mut command, lcb_STORE_OPERATION_LCB_STORE_REPLACE);
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
        lcb_cmdstore_key(command, id.as_ptr(), id_len);
        lcb_cmdstore_value(command, value.as_ptr(), value_len);

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

/// Encodes a `SearchRequest` into its libcouchbase `lcb_CMDSEARCH` representation.
pub fn encode_search(instance: *mut lcb_INSTANCE, mut request: SearchRequest) {
    request.options.index = Some(request.index);
    request.options.query = Some(request.query);

    let (payload_len, payload) = into_cstring(serde_json::to_vec(&request.options).unwrap());

    let (meta_sender, meta_receiver) = futures::channel::oneshot::channel();
    let (rows_sender, rows_receiver) = futures::channel::mpsc::unbounded();
    let cookie = Box::into_raw(Box::new(SearchCookie {
        sender: Some(request.sender),
        meta_sender,
        meta_receiver: Some(meta_receiver),
        rows_sender,
        rows_receiver: Some(rows_receiver),
    }));

    let mut command: *mut lcb_CMDSEARCH = ptr::null_mut();
    unsafe {
        lcb_cmdsearch_create(&mut command);
        lcb_cmdsearch_payload(command, payload.as_ptr(), payload_len);
        lcb_cmdsearch_callback(command, Some(search_callback));
        lcb_search(instance, cookie as *mut c_void, command);
        lcb_cmdsearch_destroy(command);
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
            idx += 1;
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
            idx += 1;
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

pub fn encode_generic_management_request(
    instance: *mut lcb_INSTANCE,
    request: GenericManagementRequest,
) {
    let (path_len, path) = into_cstring(request.path);
    let cookie = Box::into_raw(Box::new(HttpCookie::GenericManagementRequest {
        sender: request.sender,
    }));

    let (body_len, body) = into_cstring(request.payload.unwrap_or(String::from("")));
    let (content_type_len, content_type) =
        into_cstring(request.content_type.unwrap_or(String::from("")));

    let mut command: *mut lcb_CMDHTTP = ptr::null_mut();
    unsafe {
        lcb_cmdhttp_create(&mut command, lcb_HTTP_TYPE_LCB_HTTP_TYPE_MANAGEMENT);
        let method = match request.method.as_str() {
            "get" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_GET,
            "put" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_PUT,
            "post" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_POST,
            "delete" => lcb_HTTP_METHOD_LCB_HTTP_METHOD_DELETE,
            _ => panic!("Unknown HTTP method used"),
        };
        lcb_cmdhttp_method(command, method);
        lcb_cmdhttp_path(command, path.as_ptr(), path_len);

        if let Some(timeout) = request.timeout {
            lcb_cmdhttp_timeout(command, timeout.as_micros() as u32);
        }

        if content_type_len > 0 {
            lcb_cmdhttp_content_type(command, content_type.as_ptr(), content_type_len);
        }

        if body_len > 0 {
            lcb_cmdhttp_body(command, body.as_ptr(), body_len);
        }

        lcb_http(instance, cookie as *mut c_void, command);
        lcb_cmdhttp_destroy(command);
    }
}

#[cfg(feature = "volatile")]
pub fn encode_kv_stats(instance: *mut lcb_INSTANCE, request: KvStatsRequest) {
    let (scope_len, scope) = into_cstring(String::from(""));
    let (collection_len, collection) = into_cstring(String::from(""));
    let (key_len, key) = into_cstring(String::from(""));

    let key = lcb_KEYBUF {
        type_: lcb_KVBUFTYPE_LCB_KV_COPY,
        vbid: 0,
        contig: lcb_CONTIGBUF {
            bytes: key.as_ptr() as *const c_void,
            nbytes: key_len,
        },
    };

    let command = lcb_CMDSTATS {
        cmdflags: 0,
        exptime: 0,
        cas: 0,
        cid: 0,
        scope: scope.as_ptr(),
        nscope: scope_len,
        collection: collection.as_ptr(),
        ncollection: collection_len,
        key,
        timeout: 0,
        pspan: ptr::null_mut(),
    };

    let (stats_sender, stats_receiver) = futures::channel::mpsc::unbounded();
    let cookie = Box::into_raw(Box::new(crate::io::lcb::KvStatsCookie {
        sender: Some(request.sender),
        stats_sender,
        stats_receiver: Some(stats_receiver),
    }));
    unsafe {
        lcb_stats3(instance, cookie as *mut c_void, &command);
    }
}

/// Encodes a `PingRequest` into its libcouchbase `lcb_CMDPING` representation.
pub fn encode_ping(instance: *mut lcb_INSTANCE, request: PingRequest) {
    let cookie = Box::into_raw(Box::new(request.sender));

    let report_id = request
        .options
        .report_id
        .unwrap_or(Uuid::new_v4().to_hyphenated().to_string());
    let (report_id_len, c_report_id) = into_cstring(report_id);

    let mut command: *mut lcb_CMDPING = ptr::null_mut();
    unsafe {
        lcb_cmdping_create(&mut command);
        lcb_cmdping_report_id(command, c_report_id.as_ptr(), report_id_len);
        lcb_cmdping_all(command);
        lcb_ping(instance, cookie as *mut c_void, command);
        lcb_cmdping_destroy(command);
    }
}
