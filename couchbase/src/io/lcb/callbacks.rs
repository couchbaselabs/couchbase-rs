use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::results::{
    AnalyticsResult, ExistsResult, GetResult, LookupInResult, MutateInResult, MutationResult,
    QueryResult, SubDocField,
};
use crate::api::MutationToken;
use couchbase_sys::*;
use log::{debug, trace};
use serde_json::Value;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::ptr;
use std::slice::from_raw_parts;

use crate::io::lcb::{
    bucket_name_for_instance, decrement_outstanding_requests, wrapped_vsnprintf, AnalyticsCookie,
    QueryCookie,
};

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
            let bucket = {
                lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
                CStr::from_ptr(bucket_ptr).to_str().unwrap().into()
            };

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
            let bucket = {
                lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
                CStr::from_ptr(bucket_ptr).to_str().unwrap().into()
            };

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
}

fn build_kv_error_context(lcb_ctx: *const lcb_KEY_VALUE_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut key_len: usize = 0;
    let mut key_ptr: *const c_char = ptr::null();
    let key = unsafe {
        lcb_errctx_kv_key(lcb_ctx, &mut key_ptr, &mut key_len);
        CStr::from_ptr(key_ptr).to_str().unwrap().into()
    };
    ctx.insert("key", Value::String(key));

    let opaque = unsafe {
        let mut o = 0u32;
        lcb_errctx_kv_opaque(lcb_ctx, &mut o);
        o
    };
    ctx.insert("opaque", Value::Number(opaque.into()));

    let mut bucket_len: usize = 0;
    let mut bucket_ptr: *const c_char = ptr::null();
    let bucket = unsafe {
        lcb_errctx_kv_bucket(lcb_ctx, &mut bucket_ptr, &mut bucket_len);
        CStr::from_ptr(bucket_ptr).to_str().unwrap().into()
    };
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
            let collection = CStr::from_ptr(collection_ptr).to_str().unwrap().into();
            ctx.insert("collection", Value::String(collection));
        }
    }

    let mut scope_len: usize = 0;
    let mut scope_ptr: *const c_char = ptr::null();
    unsafe {
        lcb_errctx_kv_scope(lcb_ctx, &mut scope_ptr, &mut scope_len);
        if !scope_ptr.is_null() {
            let scope = CStr::from_ptr(scope_ptr).to_str().unwrap().into();
            ctx.insert("scope", Value::String(scope));
        }
    }

    let mut endpoint_len: usize = 0;
    let mut endpoint_ptr: *const c_char = ptr::null();
    unsafe {
        lcb_errctx_kv_endpoint(lcb_ctx, &mut endpoint_ptr, &mut endpoint_len);
        if !endpoint_ptr.is_null() {
            let endpoint = CStr::from_ptr(endpoint_ptr).to_str().unwrap().into();
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
    let key = unsafe {
        lcb_errctx_query_statement(lcb_ctx, &mut statement_ptr, &mut statement_len);
        CStr::from_ptr(statement_ptr).to_str().unwrap().into()
    };
    ctx.insert("statement", Value::String(key));

    ctx
}

fn build_analytics_error_context(lcb_ctx: *const lcb_ANALYTICS_ERROR_CONTEXT) -> ErrorContext {
    let mut ctx = ErrorContext::default();

    let mut statement_len: usize = 0;
    let mut statement_ptr: *const c_char = ptr::null();
    let key = unsafe {
        lcb_errctx_analytics_statement(lcb_ctx, &mut statement_ptr, &mut statement_len);
        CStr::from_ptr(statement_ptr).to_str().unwrap().into()
    };
    ctx.insert("statement", Value::String(key));

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
        lcb_STATUS_LCB_ERR_GENERIC | _ => CouchbaseError::Generic { ctx },
    }
}

fn gen_lcb_io_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, "libcouchbase error")
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

pub unsafe extern "C" fn open_callback(instance: *mut lcb_INSTANCE, err: lcb_STATUS) {
    debug!(
        "Libcouchbase notified of completed bucket open attempt for bucket {:?} (status: 0x{:x})",
        bucket_name_for_instance(instance),
        &err
    );
}
