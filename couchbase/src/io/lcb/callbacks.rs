use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::io::lcb::{HttpCookie, ViewCookie};
use crate::{
    AnalyticsResult, ExistsResult, GenericManagementResult, GetReplicaResult, GetResult,
    LookupInResult, MutateInResult, MutationResult, PingResult, PingState, QueryResult,
    SearchResult, SubDocField,
};
use couchbase_sys::*;
use log::{debug, trace};
use serde_json::Value;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::ptr;
use std::slice::from_raw_parts;
use std::str;
use std::time::Duration;

use crate::io::lcb::{
    bucket_name_for_instance, wrapped_vsnprintf, AnalyticsCookie, QueryCookie, SearchCookie,
};

use crate::io::lcb::instance::decrement_outstanding_requests;
use crate::{CounterResult, EndpointPingReport, MutationToken, ServiceType, ViewResult, ViewRow};
use std::collections::HashMap;

fn decode_and_own_str(ptr: *const c_char, len: usize) -> String {
    str::from_utf8(unsafe { from_raw_parts(ptr as *const u8, len) })
        .unwrap()
        .into()
}

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

    let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
    lcb_respstore_error_context(store_res, &mut lcb_ctx);

    let status = lcb_respstore_status(store_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_respstore_cas(store_res, &mut cas);

        let mut lcb_mutation_token = lcb_MUTATION_TOKEN {
            uuid_: 0,
            seqno_: 0,
            vbid_: 0,
        };
        lcb_respstore_mutation_token(store_res, &mut lcb_mutation_token);
        let mutation_token = if lcb_mutation_token.uuid_ != 0 {
            let mut bucket_len: usize = 0;
            let mut bucket_ptr: *const c_char = ptr::null();
            lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
            let bucket = decode_and_own_str(bucket_ptr, bucket_len);

            Some(MutationToken::new(
                lcb_mutation_token.uuid_,
                lcb_mutation_token.seqno_,
                lcb_mutation_token.vbid_,
                bucket,
            ))
        } else {
            None
        };
        Ok(MutationResult::new(cas, mutation_token))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send store result because of {:?}", e),
    }
}

pub unsafe extern "C" fn remove_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let remove_res = res as *const lcb_RESPREMOVE;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respremove_cookie(remove_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<MutationResult>>,
    );

    let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
    lcb_respremove_error_context(remove_res, &mut lcb_ctx);

    let status = lcb_respremove_status(remove_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_respremove_cas(remove_res, &mut cas);

        let mut lcb_mutation_token = lcb_MUTATION_TOKEN {
            uuid_: 0,
            seqno_: 0,
            vbid_: 0,
        };
        lcb_respremove_mutation_token(remove_res, &mut lcb_mutation_token);
        let mutation_token = if lcb_mutation_token.uuid_ != 0 {
            let mut bucket_len: usize = 0;
            let mut bucket_ptr: *const c_char = ptr::null();
            lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
            let bucket = decode_and_own_str(bucket_ptr, bucket_len);

            Some(MutationToken::new(
                lcb_mutation_token.uuid_,
                lcb_mutation_token.seqno_,
                lcb_mutation_token.vbid_,
                bucket,
            ))
        } else {
            None
        };
        Ok(MutationResult::new(cas, mutation_token))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send remove result because of {:?}", e),
    }
}

pub unsafe extern "C" fn touch_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let touch_res = res as *const lcb_RESPTOUCH;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_resptouch_cookie(touch_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<MutationResult>>,
    );

    let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
    lcb_resptouch_error_context(touch_res, &mut lcb_ctx);

    let status = lcb_resptouch_status(touch_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_resptouch_cas(touch_res, &mut cas);

        let mut lcb_mutation_token = lcb_MUTATION_TOKEN {
            uuid_: 0,
            seqno_: 0,
            vbid_: 0,
        };
        lcb_resptouch_mutation_token(touch_res, &mut lcb_mutation_token);
        let mutation_token = if lcb_mutation_token.uuid_ != 0 {
            let mut bucket_len: usize = 0;
            let mut bucket_ptr: *const c_char = ptr::null();
            lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
            let bucket = decode_and_own_str(bucket_ptr, bucket_len);

            Some(MutationToken::new(
                lcb_mutation_token.uuid_,
                lcb_mutation_token.seqno_,
                lcb_mutation_token.vbid_,
                bucket,
            ))
        } else {
            None
        };
        Ok(MutationResult::new(cas, mutation_token))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send touch result because of {:?}", e),
    }
}

pub unsafe extern "C" fn unlock_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let unlock_res = res as *const lcb_RESPUNLOCK;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respunlock_cookie(unlock_res, &mut cookie_ptr);
    let sender =
        Box::from_raw(cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<()>>);

    let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
    lcb_respunlock_error_context(unlock_res, &mut lcb_ctx);

    let status = lcb_respunlock_status(unlock_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        Ok(())
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send unlock result because of {:?}", e),
    }
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
        let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
        lcb_respget_error_context(get_res, &mut lcb_ctx);
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };

    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send get result because of {:?}", e),
    }
}

pub unsafe extern "C" fn get_replica_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    let getreplica_res = res as *const lcb_RESPGETREPLICA;
    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respgetreplica_cookie(getreplica_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<GetReplicaResult>>,
    );

    let status = lcb_respgetreplica_status(getreplica_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        let mut flags: u32 = 0;
        let mut value_len: usize = 0;
        let mut value_ptr: *const c_char = ptr::null();
        lcb_respgetreplica_cas(getreplica_res, &mut cas);
        lcb_respgetreplica_flags(getreplica_res, &mut flags);
        lcb_respgetreplica_value(getreplica_res, &mut value_ptr, &mut value_len);
        let is_active = lcb_respgetreplica_is_active(getreplica_res) != 0;
        let value = from_raw_parts(value_ptr as *const u8, value_len);
        Ok(GetReplicaResult::new(
            value.to_vec(),
            cas,
            flags,
            !is_active,
        ))
    } else {
        let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
        lcb_respgetreplica_error_context(getreplica_res, &mut lcb_ctx);
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    if result.is_ok() || (result.is_err() && lcb_respgetreplica_is_final(getreplica_res) != 0) {
        match sender.send(result) {
            Ok(_) => {}
            Err(e) => trace!("Failed to send getreplica result because of {:?}", e),
        }
        decrement_outstanding_requests(instance);
    }
}

pub unsafe extern "C" fn exists_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let exists_res = res as *const lcb_RESPEXISTS;
    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respexists_cookie(exists_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<ExistsResult>>,
    );

    let status = lcb_respexists_status(exists_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let found = lcb_respexists_is_found(exists_res);
        Ok(if found != 0 {
            let mut cas: u64 = 0;
            lcb_respexists_cas(exists_res, &mut cas);
            ExistsResult::new(true, Some(cas))
        } else {
            ExistsResult::new(false, None)
        })
    } else {
        let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
        lcb_respexists_error_context(exists_res, &mut lcb_ctx);
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send exists result because of {:?}", e),
    }
}

pub unsafe extern "C" fn lookup_in_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let subdoc_res = res as *const lcb_RESPSUBDOC;
    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respsubdoc_cookie(subdoc_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<LookupInResult>>,
    );

    let status = lcb_respsubdoc_status(subdoc_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let total_size = lcb_respsubdoc_result_size(subdoc_res);
        let mut fields = vec![];
        for i in 0..total_size {
            let status = lcb_respsubdoc_result_status(subdoc_res, i);
            let mut value_len: usize = 0;
            let mut value_ptr: *const c_char = ptr::null();
            lcb_respsubdoc_result_value(subdoc_res, i, &mut value_ptr, &mut value_len);
            let value = from_raw_parts(value_ptr as *const u8, value_len);
            fields.push(SubDocField {
                status,
                value: value.into(),
            });
        }
        let mut cas: u64 = 0;
        lcb_respsubdoc_cas(subdoc_res, &mut cas);
        Ok(LookupInResult::new(fields, cas))
    } else {
        let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
        lcb_respsubdoc_error_context(subdoc_res, &mut lcb_ctx);
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send lookup in result because of {:?}", e),
    }
}

pub unsafe extern "C" fn mutate_in_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let subdoc_res = res as *const lcb_RESPSUBDOC;
    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respsubdoc_cookie(subdoc_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<MutateInResult>>,
    );

    let status = lcb_respsubdoc_status(subdoc_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let total_size = lcb_respsubdoc_result_size(subdoc_res);
        let mut fields = vec![];
        for i in 0..total_size {
            let status = lcb_respsubdoc_result_status(subdoc_res, i);
            let mut value_len: usize = 0;
            let mut value_ptr: *const c_char = ptr::null();
            lcb_respsubdoc_result_value(subdoc_res, i, &mut value_ptr, &mut value_len);
            let value = from_raw_parts(value_ptr as *const u8, value_len);
            fields.push(SubDocField {
                status,
                value: value.into(),
            });
        }
        let mut cas: u64 = 0;
        lcb_respsubdoc_cas(subdoc_res, &mut cas);
        Ok(MutateInResult::new(fields, cas))
    } else {
        let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
        lcb_respsubdoc_error_context(subdoc_res, &mut lcb_ctx);
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send mutate in result because of {:?}", e),
    }
}

pub unsafe extern "C" fn counter_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let counter_res = res as *const lcb_RESPCOUNTER;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respcounter_cookie(counter_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<CounterResult>>,
    );

    let mut lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT = ptr::null();
    lcb_respcounter_error_context(counter_res, &mut lcb_ctx);

    let status = lcb_respcounter_status(counter_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let mut cas: u64 = 0;
        lcb_respcounter_cas(counter_res, &mut cas);

        let mut lcb_mutation_token = lcb_MUTATION_TOKEN {
            uuid_: 0,
            seqno_: 0,
            vbid_: 0,
        };
        lcb_respcounter_mutation_token(counter_res, &mut lcb_mutation_token);
        let mutation_token = if lcb_mutation_token.uuid_ != 0 {
            let mut bucket_len: usize = 0;
            let mut bucket_ptr: *const c_char = ptr::null();
            lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
            let bucket = decode_and_own_str(bucket_ptr, bucket_len);

            Some(MutationToken::new(
                lcb_mutation_token.uuid_,
                lcb_mutation_token.seqno_,
                lcb_mutation_token.vbid_,
                bucket,
            ))
        } else {
            None
        };

        let mut value: u64 = 0;
        lcb_respcounter_value(counter_res, &mut value);
        Ok(CounterResult::new(cas, mutation_token, value))
    } else {
        Err(couchbase_error_from_lcb_status(
            status,
            build_kv_error_context(lcb_ctx),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send remove result because of {:?}", e),
    }
}

fn build_kv_error_context(lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut key_len: usize = 0;
    let mut key_ptr: *const c_char = ptr::null();
    unsafe { lcb_errctx_kv_key(lcb_ctx, &mut key_ptr, &mut key_len) };
    let key = decode_and_own_str(key_ptr, key_len);
    ctx.insert("key", Value::String(key));

    let opaque = unsafe {
        let mut o = 0u32;
        lcb_errctx_kv_opaque(lcb_ctx, &mut o);
        o
    };
    ctx.insert("opaque", Value::Number(opaque.into()));

    let mut bucket_len: usize = 0;
    let mut bucket_ptr: *const c_char = ptr::null();

    unsafe { lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len) };
    let bucket = decode_and_own_str(bucket_ptr, bucket_len);
    ctx.insert("bucket", Value::String(bucket));

    let cas = unsafe {
        let mut o = 0u64;
        lcb_errctx_kv_cas(lcb_ctx, &mut o);
        o
    };
    if cas != 0 {
        ctx.insert("cas", Value::Number(cas.into()));
    }

    let mut collection_len: usize = 0;
    let mut collection_ptr: *const c_char = ptr::null();
    unsafe {
        lcb_errctx_kv_collection(lcb_ctx, &mut collection_ptr, &mut collection_len);
        if !collection_ptr.is_null() {
            let collection = decode_and_own_str(collection_ptr, collection_len);
            ctx.insert("collection", Value::String(collection));
        }
    }

    let mut scope_len: usize = 0;
    let mut scope_ptr: *const c_char = ptr::null();
    unsafe {
        lcb_errctx_kv_scope(lcb_ctx, &mut scope_ptr, &mut scope_len);
        if !scope_ptr.is_null() {
            let scope = decode_and_own_str(scope_ptr, scope_len);
            ctx.insert("scope", Value::String(scope));
        }
    }

    let mut endpoint_len: usize = 0;
    let mut endpoint_ptr: *const c_char = ptr::null();
    unsafe {
        lcb_errctx_kv_endpoint(lcb_ctx, &mut endpoint_ptr, &mut endpoint_len);
        if !endpoint_ptr.is_null() && endpoint_len > 0 {
            // Looks like the endpoint is 0 terminated, as opposed to bucket etc...
            let endpoint = decode_and_own_str(endpoint_ptr, endpoint_len);
            ctx.insert("remote", Value::String(endpoint));
        }
    }

    let status = unsafe {
        let mut o = 0u16;
        lcb_errctx_kv_status_code(lcb_ctx, &mut o);
        o
    };
    ctx.insert("status", Value::Number(status.into()));

    ctx
}

fn build_query_error_context(lcb_ctx: *const lcb_QUERY_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut statement_len: usize = 0;
    let mut statement_ptr: *const c_char = ptr::null();
    let statement = unsafe {
        lcb_errctx_query_statement(lcb_ctx, &mut statement_ptr, &mut statement_len);
        decode_and_own_str(statement_ptr, statement_len)
    };
    ctx.insert("statement", Value::String(statement));

    ctx
}

fn build_analytics_error_context(lcb_ctx: *const lcb_ANALYTICS_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut statement_len: usize = 0;
    let mut statement_ptr: *const c_char = ptr::null();
    let statement = unsafe {
        lcb_errctx_analytics_statement(lcb_ctx, &mut statement_ptr, &mut statement_len);
        decode_and_own_str(statement_ptr, statement_len)
    };
    ctx.insert("statement", Value::String(statement));

    ctx
}

fn build_search_error_context(lcb_ctx: *const lcb_SEARCH_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut query_len: usize = 0;
    let mut query_ptr: *const c_char = ptr::null();
    let query = unsafe {
        lcb_errctx_search_query(lcb_ctx, &mut query_ptr, &mut query_len);
        decode_and_own_str(query_ptr, query_len)
    };
    ctx.insert("query", Value::String(query));

    ctx
}

fn build_view_error_context(lcb_ctx: *const lcb_VIEW_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut ddoc_len: usize = 0;
    let mut ddoc_ptr: *const c_char = ptr::null();
    let ddoc = unsafe {
        lcb_errctx_view_design_document(lcb_ctx, &mut ddoc_ptr, &mut ddoc_len);
        decode_and_own_str(ddoc_ptr, ddoc_len)
    };
    ctx.insert("design_document_name", Value::String(ddoc));

    let mut view_len: usize = 0;
    let mut view_ptr: *const c_char = ptr::null();
    let view = unsafe {
        lcb_errctx_view_design_document(lcb_ctx, &mut view_ptr, &mut view_len);
        decode_and_own_str(view_ptr, view_len)
    };
    ctx.insert("view_name", Value::String(view));

    ctx
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

    let status = lcb_respquery_status(res);

    if cookie.sender.is_some() {
        let response = if status != 0 {
            let mut lcb_ctx: *const lcb_QUERY_ERROR_CONTEXT = ptr::null();
            lcb_respquery_error_context(res, &mut lcb_ctx);
            Err(couchbase_error_from_lcb_status(
                status,
                build_query_error_context(lcb_ctx),
            ))
        } else {
            Ok(QueryResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
            ))
        };

        match cookie
            .sender
            .take()
            .expect("Could not take result!")
            .send(response)
        {
            Ok(_) => {}
            Err(e) => trace!("Failed to send query result because of {:?}", e),
        }
    }

    if lcb_respquery_is_final(res) != 0 {
        cookie.rows_sender.close_channel();

        if status == 0 {
            match cookie
                .meta_sender
                .send(serde_json::from_slice(row).unwrap())
            {
                Ok(_) => {}
                Err(e) => trace!("Failed to send query meta data because of {:?}", e),
            }
        }

        decrement_outstanding_requests(instance);
    } else {
        match cookie.rows_sender.unbounded_send(row.to_vec()) {
            Ok(_) => {}
            Err(e) => trace!("Failed to send query row because of {:?}", e),
        }
        Box::into_raw(cookie);
    }
}

pub unsafe extern "C" fn analytics_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPANALYTICS,
) {
    let mut row_len: usize = 0;
    let mut row_ptr: *const c_char = ptr::null();
    lcb_respanalytics_row(res, &mut row_ptr, &mut row_len);
    let row = from_raw_parts(row_ptr as *const u8, row_len);

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respanalytics_cookie(res, &mut cookie_ptr);
    let mut cookie = Box::from_raw(cookie_ptr as *mut AnalyticsCookie);

    let status = lcb_respanalytics_status(res);

    if cookie.sender.is_some() {
        let response = if status != 0 {
            let mut lcb_ctx: *const lcb_ANALYTICS_ERROR_CONTEXT = ptr::null();
            lcb_respanalytics_error_context(res, &mut lcb_ctx);
            Err(couchbase_error_from_lcb_status(
                status,
                build_analytics_error_context(lcb_ctx),
            ))
        } else {
            Ok(AnalyticsResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
            ))
        };

        match cookie
            .sender
            .take()
            .expect("Could not take result!")
            .send(response)
        {
            Ok(_) => {}
            Err(e) => trace!("Failed to send analytics result because of {:?}", e),
        }
    }

    if lcb_respanalytics_is_final(res) != 0 {
        cookie.rows_sender.close_channel();

        if status == 0 {
            match cookie
                .meta_sender
                .send(serde_json::from_slice(row).unwrap())
            {
                Ok(_) => {}
                Err(e) => trace!("Failed to send analytics meta data ecause of {:?}", e),
            }
        }

        decrement_outstanding_requests(instance);
    } else {
        match cookie.rows_sender.unbounded_send(row.to_vec()) {
            Ok(_) => {}
            Err(e) => trace!("Failed to send analytics row because of {:?}", e),
        }
        Box::into_raw(cookie);
    }
}

pub unsafe extern "C" fn search_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPSEARCH,
) {
    let mut row_len: usize = 0;
    let mut row_ptr: *const c_char = ptr::null();
    lcb_respsearch_row(res, &mut row_ptr, &mut row_len);
    let row = from_raw_parts(row_ptr as *const u8, row_len);

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respsearch_cookie(res, &mut cookie_ptr);
    let mut cookie = Box::from_raw(cookie_ptr as *mut SearchCookie);

    let status = lcb_respsearch_status(res);

    if cookie.sender.is_some() {
        let response = if status != 0 {
            let mut lcb_ctx: *const lcb_SEARCH_ERROR_CONTEXT = ptr::null();
            lcb_respsearch_error_context(res, &mut lcb_ctx);
            Err(couchbase_error_from_lcb_status(
                status,
                build_search_error_context(lcb_ctx),
            ))
        } else {
            Ok(SearchResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
                cookie.facet_receiver.take().unwrap(),
            ))
        };

        match cookie
            .sender
            .take()
            .expect("Could not take result!")
            .send(response)
        {
            Ok(_) => {}
            Err(e) => trace!("Failed to send search result because of {:?}", e),
        }
    }

    if lcb_respsearch_is_final(res) != 0 {
        cookie.rows_sender.close_channel();

        if status == 0 {
            let meta = serde_json::from_slice::<Value>(row).unwrap();
            if let Some(f) = meta.as_object().unwrap().get("facets") {
                match cookie.facet_sender.send(f.clone()) {
                    Ok(_) => {}
                    Err(e) => trace!("Failed to send search meta data ecause of {:?}", e),
                }
            }

            match cookie
                .meta_sender
                .send(serde_json::from_value(meta).unwrap())
            {
                Ok(_) => {}
                Err(e) => trace!("Failed to send search meta data ecause of {:?}", e),
            }
        }

        decrement_outstanding_requests(instance);
    } else {
        match cookie.rows_sender.unbounded_send(row.to_vec()) {
            Ok(_) => {}
            Err(e) => trace!("Failed to send search row because of {:?}", e),
        }
        Box::into_raw(cookie);
    }
}

pub unsafe extern "C" fn view_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPVIEW,
) {
    let mut id_len: usize = 0;
    let mut id_ptr: *const c_char = ptr::null();
    lcb_respview_doc_id(res, &mut id_ptr, &mut id_len);
    let doc_id = from_raw_parts(id_ptr as *const u8, id_len);
    let mut key_len: usize = 0;
    let mut key_ptr: *const c_char = ptr::null();
    lcb_respview_key(res, &mut key_ptr, &mut key_len);
    let key = from_raw_parts(key_ptr as *const u8, key_len);
    let mut row_len: usize = 0;
    let mut row_ptr: *const c_char = ptr::null();
    lcb_respview_row(res, &mut row_ptr, &mut row_len);
    let row = from_raw_parts(row_ptr as *const u8, row_len);

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respview_cookie(res, &mut cookie_ptr);
    let mut cookie = Box::from_raw(cookie_ptr as *mut ViewCookie);

    let status = lcb_respview_status(res);

    if cookie.sender.is_some() {
        let response = if status != 0 {
            let mut lcb_ctx: *const lcb_VIEW_ERROR_CONTEXT = ptr::null();
            lcb_respview_error_context(res, &mut lcb_ctx);
            Err(couchbase_error_from_lcb_status(
                status,
                build_view_error_context(lcb_ctx),
            ))
        } else {
            Ok(ViewResult::new(
                cookie.rows_receiver.take().unwrap(),
                cookie.meta_receiver.take().unwrap(),
            ))
        };

        match cookie
            .sender
            .take()
            .expect("Could not take result!")
            .send(response)
        {
            Ok(_) => {}
            Err(e) => trace!("Failed to send view result because of {:?}", e),
        }
    }

    if lcb_respview_is_final(res) != 0 {
        cookie.rows_sender.close_channel();

        if status == 0 {
            match cookie
                .meta_sender
                .send(serde_json::from_slice(row).unwrap())
            {
                Ok(_) => {}
                Err(e) => trace!("Failed to send view meta data because of {:?}", e),
            }
        }

        decrement_outstanding_requests(instance);
    } else {
        let mut id: Option<String> = None;
        if !doc_id.is_empty() {
            id = Some(str::from_utf8(doc_id).unwrap().to_string());
        }
        match cookie.rows_sender.unbounded_send(ViewRow {
            id,
            key: key.to_vec(),
            value: row.to_vec(),
        }) {
            Ok(_) => {}
            Err(e) => trace!("Failed to send search row because of {:?}", e),
        }
        Box::into_raw(cookie);
    }
}

#[allow(non_upper_case_globals)]
pub fn couchbase_error_from_lcb_status(status: lcb_STATUS, ctx: ErrorContext) -> CouchbaseError {
    match status {
        lcb_STATUS_LCB_ERR_DOCUMENT_NOT_FOUND => CouchbaseError::DocumentNotFound { ctx },
        lcb_STATUS_LCB_ERR_TIMEOUT | lcb_STATUS_LCB_ERR_AMBIGUOUS_TIMEOUT => {
            CouchbaseError::Timeout {
                ambiguous: true,
                ctx,
            }
        }
        lcb_STATUS_LCB_ERR_UNAMBIGUOUS_TIMEOUT => CouchbaseError::Timeout {
            ambiguous: false,
            ctx,
        },
        lcb_STATUS_LCB_ERR_INVALID_ARGUMENT => CouchbaseError::InvalidArgument { ctx },
        lcb_STATUS_LCB_ERR_CAS_MISMATCH => CouchbaseError::CasMismatch { ctx },
        lcb_STATUS_LCB_ERR_REQUEST_CANCELED => CouchbaseError::RequestCanceled { ctx },
        lcb_STATUS_LCB_ERR_SERVICE_NOT_AVAILABLE => CouchbaseError::ServiceNotAvailable { ctx },
        lcb_STATUS_LCB_ERR_INTERNAL_SERVER_FAILURE => CouchbaseError::InternalServerFailure { ctx },
        lcb_STATUS_LCB_ERR_AUTHENTICATION_FAILURE => CouchbaseError::AuthenticationFailure { ctx },
        lcb_STATUS_LCB_ERR_TEMPORARY_FAILURE => CouchbaseError::TemporaryFailure { ctx },
        lcb_STATUS_LCB_ERR_PARSING_FAILURE => CouchbaseError::ParsingFailure { ctx },
        lcb_STATUS_LCB_ERR_BUCKET_NOT_FOUND => CouchbaseError::BucketNotFound { ctx },
        lcb_STATUS_LCB_ERR_COLLECTION_NOT_FOUND => CouchbaseError::CollectionNotFound { ctx },
        lcb_STATUS_LCB_ERR_ENCODING_FAILURE => CouchbaseError::EncodingFailure {
            ctx,
            source: gen_lcb_io_error(),
        },
        lcb_STATUS_LCB_ERR_DECODING_FAILURE => CouchbaseError::DecodingFailure {
            ctx,
            source: gen_lcb_io_error(),
        },
        lcb_STATUS_LCB_ERR_UNSUPPORTED_OPERATION => CouchbaseError::UnsupportedOperation { ctx },
        lcb_STATUS_LCB_ERR_SCOPE_NOT_FOUND => CouchbaseError::ScopeNotFound { ctx },
        lcb_STATUS_LCB_ERR_INDEX_NOT_FOUND => CouchbaseError::IndexNotFound { ctx },
        lcb_STATUS_LCB_ERR_INDEX_EXISTS => CouchbaseError::IndexExists { ctx },
        lcb_STATUS_LCB_ERR_DOCUMENT_UNRETRIEVABLE => CouchbaseError::DocumentUnretrievable { ctx },
        lcb_STATUS_LCB_ERR_DOCUMENT_LOCKED => CouchbaseError::DocumentLocked { ctx },
        lcb_STATUS_LCB_ERR_VALUE_TOO_LARGE => CouchbaseError::ValueTooLarge { ctx },
        lcb_STATUS_LCB_ERR_DOCUMENT_EXISTS => CouchbaseError::DocumentExists { ctx },
        lcb_STATUS_LCB_ERR_VALUE_NOT_JSON => CouchbaseError::ValueNotJson { ctx },
        lcb_STATUS_LCB_ERR_DURABILITY_LEVEL_NOT_AVAILABLE => {
            CouchbaseError::DurabilityLevelNotAvailable { ctx }
        }
        lcb_STATUS_LCB_ERR_DURABILITY_IMPOSSIBLE => CouchbaseError::DurabilityImpossible { ctx },
        lcb_STATUS_LCB_ERR_DURABILITY_AMBIGUOUS => CouchbaseError::DurabilityAmbiguous { ctx },
        lcb_STATUS_LCB_ERR_DURABLE_WRITE_IN_PROGRESS => {
            CouchbaseError::DurableWriteInProgress { ctx }
        }
        lcb_STATUS_LCB_ERR_DURABLE_WRITE_RE_COMMIT_IN_PROGRESS => {
            CouchbaseError::DurableWriteReCommitInProgress { ctx }
        }
        lcb_STATUS_LCB_ERR_MUTATION_LOST => CouchbaseError::MutationLost { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_NOT_FOUND => CouchbaseError::PathNotFound { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_MISMATCH => CouchbaseError::PathMismatch { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_INVALID => CouchbaseError::PathInvalid { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_TOO_BIG => CouchbaseError::PathTooBig { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_TOO_DEEP => CouchbaseError::PathTooDeep { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_VALUE_TOO_DEEP => CouchbaseError::ValueTooDeep { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_VALUE_INVALID => CouchbaseError::ValueInvalid { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_DOCUMENT_NOT_JSON => CouchbaseError::DocumentNotJson { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_NUMBER_TOO_BIG => CouchbaseError::NumberTooBig { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_DELTA_INVALID => CouchbaseError::DeltaInvalid { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_PATH_EXISTS => CouchbaseError::PathExists { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_UNKNOWN_MACRO => CouchbaseError::XattrUnknownMacro { ctx },
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_INVALID_FLAG_COMBO => {
            CouchbaseError::XattrInvalidFlagCombo { ctx }
        }
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_INVALID_KEY_COMBO => {
            CouchbaseError::XattrInvalidKeyCombo { ctx }
        }
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_UNKNOWN_VIRTUAL_ATTRIBUTE => {
            CouchbaseError::XattrUnknownVirtualAttribute { ctx }
        }
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_CANNOT_MODIFY_VIRTUAL_ATTRIBUTE => {
            CouchbaseError::XattrCannotModifyVirtualAttribute { ctx }
        }
        lcb_STATUS_LCB_ERR_SUBDOC_XATTR_INVALID_ORDER => CouchbaseError::XattrInvalidOrder { ctx },
        lcb_STATUS_LCB_ERR_PLANNING_FAILURE => CouchbaseError::PlanningFailure { ctx },
        lcb_STATUS_LCB_ERR_INDEX_FAILURE => CouchbaseError::IndexFailure { ctx },
        lcb_STATUS_LCB_ERR_PREPARED_STATEMENT_FAILURE => {
            CouchbaseError::PreparedStatementFailure { ctx }
        }
        lcb_STATUS_LCB_ERR_COMPILATION_FAILED => CouchbaseError::CompilationFailure { ctx },
        lcb_STATUS_LCB_ERR_JOB_QUEUE_FULL => CouchbaseError::JobQueueFull { ctx },
        lcb_STATUS_LCB_ERR_DATASET_NOT_FOUND => CouchbaseError::DatasetNotFound { ctx },
        lcb_STATUS_LCB_ERR_DATAVERSE_NOT_FOUND => CouchbaseError::DataverseNotFound { ctx },
        lcb_STATUS_LCB_ERR_DATASET_EXISTS => CouchbaseError::DatasetExists { ctx },
        lcb_STATUS_LCB_ERR_DATAVERSE_EXISTS => CouchbaseError::DataverseExists { ctx },
        lcb_STATUS_LCB_ERR_ANALYTICS_LINK_NOT_FOUND => CouchbaseError::LinkNotFound { ctx },
        lcb_STATUS_LCB_ERR_VIEW_NOT_FOUND => CouchbaseError::ViewNotFound { ctx },
        lcb_STATUS_LCB_ERR_DESIGN_DOCUMENT_NOT_FOUND => {
            CouchbaseError::DesignDocumentNotFound { ctx }
        }
        lcb_STATUS_LCB_ERR_COLLECTION_ALREADY_EXISTS => CouchbaseError::CollectionExists { ctx },
        lcb_STATUS_LCB_ERR_SCOPE_EXISTS => CouchbaseError::ScopeExists { ctx },
        lcb_STATUS_LCB_ERR_USER_NOT_FOUND => CouchbaseError::UserNotFound { ctx },
        lcb_STATUS_LCB_ERR_GROUP_NOT_FOUND => CouchbaseError::GroupNotFound { ctx },
        lcb_STATUS_LCB_ERR_BUCKET_ALREADY_EXISTS => CouchbaseError::BucketExists { ctx },
        _ => CouchbaseError::Generic { ctx },
    }
}

fn gen_lcb_io_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, "libcouchbase error")
}

const LOG_MSG_LENGTH: usize = 1024;

// Windows disagrees with Linux and Macos on which type is available.
#[cfg(all(
    not(target_os = "windows"),
    all(not(target_arch = "aarch64"), not(target_arch = "arm64"))
))]
pub(crate) type VaList = *mut __va_list_tag;
#[cfg(all(
    not(target_os = "windows"),
    any(target_arch = "aarch64", target_arch = "arm64")
))]
pub(crate) type VaList = va_list;
#[cfg(target_os = "windows")]
pub(crate) type VaList = va_list;

pub unsafe extern "C" fn logger_callback(
    _procs: *const lcb_LOGGER,
    _iid: u64,
    _subsys: *const c_char,
    severity: lcb_LOG_SEVERITY,
    _srcfile: *const c_char,
    _srcline: c_int,
    fmt: *const c_char,
    ap: VaList,
) {
    let level = match severity {
        0 => log::Level::Trace,
        1 => log::Level::Debug,
        2 => log::Level::Info,
        3 => log::Level::Warn,
        _ => log::Level::Error,
    };

    let mut target_buffer = [0u8; LOG_MSG_LENGTH];

    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    let buf = &mut target_buffer[0] as *mut u8;
    #[cfg(not(all(target_arch = "aarch64", target_os = "linux")))]
    let buf = &mut target_buffer[0] as *mut u8 as *mut i8;

    let result = wrapped_vsnprintf(buf, LOG_MSG_LENGTH as c_uint, fmt, ap) as usize;
    let range_end = if result < target_buffer.len() {
        result + 1
    } else {
        target_buffer.len()
    };
    let decoded = CStr::from_bytes_with_nul(&target_buffer[0..range_end]).unwrap();

    log::log!(level, "{}", decoded.to_str().unwrap());
}

pub unsafe extern "C" fn open_callback(instance: *mut lcb_INSTANCE, err: lcb_STATUS) {
    debug!(
        "Libcouchbase notified of completed bucket open attempt for bucket {:?} (status: 0x{:x})",
        bucket_name_for_instance(instance),
        &err
    );
}

pub unsafe extern "C" fn http_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let http_res = res as *const lcb_RESPHTTP;

    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_resphttp_cookie(http_res, &mut cookie_ptr);
    let cookie = Box::from_raw(cookie_ptr as *mut HttpCookie);

    match *cookie {
        HttpCookie::GenericManagementRequest { sender: s } => {
            if lcb_resphttp_is_final(http_res) != 0 {
                let status = {
                    let mut o = 0u16;
                    lcb_resphttp_http_status(http_res, &mut o);
                    o
                };

                let mut body_len: usize = 0;
                let mut body_ptr: *const c_char = ptr::null();
                lcb_resphttp_body(http_res, &mut body_ptr, &mut body_len);
                let row = from_raw_parts(body_ptr as *const u8, body_len).to_vec();
                let payload = if row.is_empty() { None } else { Some(row) };
                s.send(Ok(GenericManagementResult::new(status, payload)))
                    .unwrap();
            }
        }
    }
}

pub unsafe extern "C" fn ping_callback(
    instance: *mut lcb_INSTANCE,
    _cbtype: i32,
    res: *const lcb_RESPBASE,
) {
    decrement_outstanding_requests(instance);
    let ping_res = res as *const lcb_RESPPING;
    let mut cookie_ptr: *mut c_void = ptr::null_mut();
    lcb_respping_cookie(ping_res, &mut cookie_ptr);
    let sender = Box::from_raw(
        cookie_ptr as *mut futures::channel::oneshot::Sender<CouchbaseResult<PingResult>>,
    );

    let mut services: HashMap<ServiceType, Vec<EndpointPingReport>> = HashMap::new();

    let status = lcb_respping_status(ping_res);
    let result = if status == lcb_STATUS_LCB_SUCCESS {
        let result_size = lcb_respping_result_size(ping_res);

        for i in 0..result_size {
            let mut svc = lcb_PING_SERVICE_LCB_PING_SERVICE__MAX;
            lcb_respping_result_service(ping_res, i, &mut svc);

            let lcb_status = lcb_respping_result_status(ping_res, i);

            let service_type = match svc {
                0 => ServiceType::KeyValue,
                1 => ServiceType::Views,
                2 => ServiceType::Query,
                3 => ServiceType::Search,
                4 => ServiceType::Analytics,
                _ => continue,
            };

            let status = match lcb_status {
                0 => PingState::OK,
                1 => PingState::Timeout,
                2 => PingState::Error,
                _ => PingState::Invalid,
            };

            let mut id_len: usize = 0;
            let mut id_ptr: *const c_char = ptr::null();
            lcb_respping_result_id(ping_res, i, &mut id_ptr, &mut id_len);
            let id = decode_and_own_str(id_ptr, id_len);

            let mut local_len: usize = 0;
            let mut local_ptr: *const c_char = ptr::null();
            lcb_respping_result_local(ping_res, i, &mut local_ptr, &mut local_len);
            let local = match local_ptr.is_null() {
                true => None,
                false => Some(decode_and_own_str(local_ptr, local_len)),
            };

            let mut remote_len: usize = 0;
            let mut remote_ptr: *const c_char = ptr::null();
            lcb_respping_result_remote(ping_res, i, &mut remote_ptr, &mut remote_len);
            let remote = match remote_ptr.is_null() {
                true => None,
                false => Some(decode_and_own_str(remote_ptr, remote_len)),
            };

            let scope = match service_type {
                ServiceType::KeyValue => {
                    let mut scope_len: usize = 0;
                    let mut scope_ptr: *const c_char = ptr::null();
                    lcb_respping_result_scope(ping_res, i, &mut scope_ptr, &mut scope_len);
                    Some(decode_and_own_str(scope_ptr, scope_len))
                }
                _ => None,
            };

            let error = match lcb_status {
                0 => None,
                1 => Some(String::from("Timeout")),
                _ => {
                    let lcb_error = CStr::from_ptr(lcb_strerror_long(lcb_status));
                    Some(lcb_error.to_str().unwrap().into())
                }
            };

            let mut latency: u64 = 0;
            lcb_respping_result_latency(ping_res, i, &mut latency);

            let service = services.entry(service_type).or_insert_with(Vec::new);

            service.push(EndpointPingReport::new(
                local,
                remote,
                status,
                error,
                Duration::from_micros(latency),
                scope,
                id,
                service_type,
            ))
        }

        Ok(PingResult::new(String::from(""), services))
    } else {
        // let lcb_error = unsafe { CStr::from_ptr(lcb_strerror_long(status)) };
        // let error: String = lcb_error.to_str().unwrap().into();
        Err(couchbase_error_from_lcb_status(
            status,
            ErrorContext::default(),
        ))
    };
    match sender.send(result) {
        Ok(_) => {}
        Err(e) => trace!("Failed to send exists result because of {:?}", e),
    }
}
