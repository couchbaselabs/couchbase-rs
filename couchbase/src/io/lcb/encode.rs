use crate::io::lcb::callbacks::{
    analytics_callback, query_callback, search_callback, view_callback,
};
use crate::io::lcb::{AnalyticsCookie, HttpCookie, QueryCookie, SearchCookie, ViewCookie};
use crate::io::request::*;
use crate::{
    CouchbaseResult, DurabilityLevel, ErrorContext, LookupInSpec, MutateInSpec, ReplicaMode,
    ServiceType, StoreSemantics,
};
use futures::channel::oneshot::Sender;
use log::{debug, warn};
use serde_json::Value;
use std::convert::TryInto;

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

impl TryInto<lcb_DURABILITY_LEVEL> for DurabilityLevel {
    type Error = EncodeFailure;

    fn try_into(self) -> Result<lcb_DURABILITY_LEVEL, Self::Error> {
        let level = match self {
            DurabilityLevel::None => lcb_DURABILITY_LEVEL_LCB_DURABILITYLEVEL_NONE,
            DurabilityLevel::Majority => lcb_DURABILITY_LEVEL_LCB_DURABILITYLEVEL_MAJORITY,
            DurabilityLevel::MajorityAndPersistOnMaster => {
                lcb_DURABILITY_LEVEL_LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE
            }
            DurabilityLevel::PersistToMajority => {
                lcb_DURABILITY_LEVEL_LCB_DURABILITYLEVEL_PERSIST_TO_MAJORITY
            }
            DurabilityLevel::ClientVerified(_) => {
                panic!("Enhanced durability not supported for client verified durability, this is probably a bug :(")
            }
        };

        Ok(level)
    }
}

impl From<ReplicaMode> for lcb_REPLICA_MODE {
    fn from(rm: ReplicaMode) -> Self {
        match rm {
            ReplicaMode::Any => lcb_REPLICA_MODE_LCB_REPLICA_MODE_ANY,
            ReplicaMode::All => lcb_REPLICA_MODE_LCB_REPLICA_MODE_ALL,
        }
    }
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
        if sender.send(Err(err)).is_err() {
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
        if sender.sender.take().unwrap().send(Err(err)).is_err() {
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
        if sender.sender.take().unwrap().send(Err(err)).is_err() {
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
        if sender.sender.take().unwrap().send(Err(err)).is_err() {
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
        if sender.sender.take().unwrap().send(Err(err)).is_err() {
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
                if sender.send(Err(err)).is_err() {
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
                    lcb_cmdget_locktime(command, lock_time.as_secs() as u32),
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
                verify(lcb_cmdget_expiry(command, expiry.as_secs() as u32), cookie)?;

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

/// Encodes a `GetReplicaRequest` into its libcouchbase `lcb_CMDGETREPLICA` representation.
/// This is a bit odd right now but will have to change once get_all_replicas is implemented.
pub fn encode_get_replica(
    instance: *mut lcb_INSTANCE,
    request: GetReplicaRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDGETREPLICA = ptr::null_mut();
    unsafe {
        verify(
            lcb_cmdgetreplica_create(&mut command, request.mode.into()),
            cookie,
        )?;
        verify(lcb_cmdgetreplica_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdgetreplica_collection(
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
                lcb_cmdgetreplica_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        verify(
            lcb_getreplica(instance, cookie as *mut c_void, command),
            cookie,
        )?;
        verify(lcb_cmdgetreplica_destroy(command), cookie)?;
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
        let durability: Option<DurabilityLevel>;
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
                if options.preserve_expiry {
                    verify(lcb_cmdstore_preserve_expiry(command, 1), cookie)?;
                }
                durability = options.durability;
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
                durability = options.durability;
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
                if options.preserve_expiry {
                    verify(lcb_cmdstore_preserve_expiry(command, 1), cookie)?;
                }
                durability = options.durability;
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
                durability = options.durability;
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
                durability = options.durability;
            }
        }

        if let Some(d) = durability {
            match d {
                DurabilityLevel::ClientVerified(cv) => {
                    let replicate_to = match cv.replicate_to {
                        Some(r) => r.into(),
                        None => 0,
                    };
                    let persist_to = match cv.persist_to {
                        Some(r) => r.into(),
                        None => 0,
                    };
                    lcb_cmdstore_durability_observe(command, persist_to, replicate_to);
                }
                _ => {
                    let lcb_level = d.try_into()?;
                    lcb_cmdstore_durability(command, lcb_level);
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

        if let Some(d) = request.options.durability {
            match d {
                DurabilityLevel::ClientVerified(_) => {
                    panic!("Client verified not supported with remove");
                }
                _ => {
                    let lcb_level = d.try_into()?;
                    lcb_cmdremove_durability(command, lcb_level);
                }
            }
        }

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

/// Encodes a `TouchRequest` into its libcouchbase `lcb_CMDTOUCH` representation.
pub fn encode_touch(
    instance: *mut lcb_INSTANCE,
    request: TouchRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDTOUCH = ptr::null_mut();
    unsafe {
        verify(lcb_cmdtouch_create(&mut command), cookie)?;
        verify(lcb_cmdtouch_key(command, id.as_ptr(), id_len), cookie)?;
        verify(
            lcb_cmdtouch_expiry(command, request.expiry.as_secs() as u32),
            cookie,
        )?;
        verify(
            lcb_cmdtouch_collection(
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
                lcb_cmdtouch_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        verify(lcb_touch(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdtouch_destroy(command), cookie)?;
    }

    Ok(())
}

/// Encodes an `UnlockRequest` into its libcouchbase `lcb_CMDUNLOCK` representation.
pub fn encode_unlock(
    instance: *mut lcb_INSTANCE,
    request: UnlockRequest,
) -> Result<(), EncodeFailure> {
    let (id_len, id) = into_cstring(request.id);
    let cookie = Box::into_raw(Box::new(request.sender));
    let (scope_len, scope) = into_cstring(request.scope);
    let (collection_len, collection) = into_cstring(request.collection);

    let mut command: *mut lcb_CMDUNLOCK = ptr::null_mut();
    unsafe {
        verify(lcb_cmdunlock_create(&mut command), cookie)?;
        verify(lcb_cmdunlock_key(command, id.as_ptr(), id_len), cookie)?;
        verify(lcb_cmdunlock_cas(command, request.cas), cookie)?;

        verify(
            lcb_cmdunlock_collection(
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
                lcb_cmdunlock_timeout(command, timeout.as_micros() as u32),
                cookie,
            )?;
        }

        verify(lcb_unlock(instance, cookie as *mut c_void, command), cookie)?;
        verify(lcb_cmdunlock_destroy(command), cookie)?;
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

        if let Some(d) = request.options.durability {
            match d {
                DurabilityLevel::ClientVerified(_) => {
                    panic!("Client verified not supported with counter");
                }
                _ => {
                    let lcb_level = d.try_into()?;
                    lcb_cmdcounter_durability(command, lcb_level);
                }
            }
        }

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
    Get {
        path_len: usize,
        path: CString,
        flags: u32,
    },
    Exists {
        path_len: usize,
        path: CString,
        flags: u32,
    },
    Count {
        path_len: usize,
        path: CString,
        flags: u32,
    },
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
            LookupInSpec::Get { path, xattr } => {
                let flags = make_subdoc_flags(None, xattr, false);
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Get {
                    path_len,
                    path,
                    flags,
                }
            }
            LookupInSpec::Exists { path, xattr } => {
                let flags = make_subdoc_flags(None, xattr, false);
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Exists {
                    path_len,
                    path,
                    flags,
                }
            }
            LookupInSpec::Count { path, xattr } => {
                let flags = make_subdoc_flags(None, xattr, false);
                let (path_len, path) = into_cstring(path);
                EncodedLookupSpec::Count {
                    path_len,
                    path,
                    flags,
                }
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

        for (idx, lookup_spec) in lookup_specs.iter().enumerate() {
            match lookup_spec {
                EncodedLookupSpec::Get {
                    path_len,
                    path,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_get(specs, idx, *flags, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedLookupSpec::Exists {
                    path_len,
                    path,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_exists(specs, idx, *flags, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedLookupSpec::Count {
                    path_len,
                    path,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_get_count(specs, idx, *flags, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
            }
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
        flags: u32,
    },
    Insert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
    Upsert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
    ArrayAddUnique {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
    Remove {
        path_len: usize,
        path: CString,
        flags: u32,
    },
    Counter {
        path_len: usize,
        path: CString,
        delta: i64,
        flags: u32,
    },
    ArrayAppend {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
    ArrayPrepend {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
    ArrayInsert {
        path_len: usize,
        path: CString,
        value_len: usize,
        value: CString,
        flags: u32,
    },
}

pub(crate) const LOOKUPIN_MACRO_CAS: &str = "$document.CAS";
pub(crate) const LOOKUPIN_MACRO_EXPIRYTIME: &str = "$document.exptime";
pub(crate) const LOOKUPIN_MACRO_FLAGS: &str = "$document.flags";

pub(crate) const MUTATION_MACRO_CAS: &str = "${Mutation.CAS}";
pub(crate) const MUTATION_MACRO_SEQNO: &str = "${Mutation.seqno}";
pub(crate) const MUTATION_MACRO_VALUE_CRC32C: &str = "${Mutation.value_crc32c}";
pub(crate) const MUTATION_MACRO_CAS_MATCHER: &str = "\"${Mutation.CAS}\"";
pub(crate) const MUTATION_MACRO_SEQNO_MATCHER: &str = "\"${Mutation.seqno}\"";
pub(crate) const MUTATION_MACRO_VALUE_CRC32C_MATCHER: &str = "\"${Mutation.value_crc32c}\"";

fn make_subdoc_flags(value: Option<&Vec<u8>>, xattr: bool, create_path: bool) -> u32 {
    let mut flags: u32 = 0;
    if xattr {
        flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
    }
    if create_path {
        flags |= LCB_SUBDOCSPECS_F_MKINTERMEDIATES;
    }
    if let Some(v) = value {
        match std::str::from_utf8(v.as_slice()) {
            Ok(str_val) => match str_val {
                MUTATION_MACRO_CAS_MATCHER => {
                    flags |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
                    flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
                }
                MUTATION_MACRO_SEQNO_MATCHER => {
                    flags |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
                    flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
                }
                MUTATION_MACRO_VALUE_CRC32C_MATCHER => {
                    flags |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
                    flags |= LCB_SUBDOCSPECS_F_XATTRPATH;
                }
                _ => {}
            },
            Err(_e) => {}
        }
    }

    flags
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
            MutateInSpec::Replace { path, value, xattr } => {
                let flags = make_subdoc_flags(Some(&value), xattr, false);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Replace {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::Insert {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Insert {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::Upsert {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::Upsert {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::ArrayAddUnique {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayAddUnique {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::Remove { path, xattr } => {
                let flags = make_subdoc_flags(None, xattr, false);
                let (path_len, path) = into_cstring(path);
                EncodedMutateSpec::Remove {
                    path_len,
                    path,
                    flags,
                }
            }
            MutateInSpec::Counter {
                path,
                delta,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(None, xattr, create_path);
                let (path_len, path) = into_cstring(path);
                EncodedMutateSpec::Counter {
                    path_len,
                    path,
                    delta,
                    flags,
                }
            }
            MutateInSpec::ArrayAppend {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayAppend {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::ArrayPrepend {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayPrepend {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                }
            }
            MutateInSpec::ArrayInsert {
                path,
                value,
                xattr,
                create_path,
            } => {
                let flags = make_subdoc_flags(Some(&value), xattr, create_path);
                let (path_len, path) = into_cstring(path);
                let (value_len, value) = into_cstring(value);
                EncodedMutateSpec::ArrayInsert {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
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

        for (idx, mutate_spec) in mutate_specs.iter().enumerate() {
            match mutate_spec {
                EncodedMutateSpec::Insert {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_dict_add(
                            specs,
                            idx,
                            *flags,
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
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_dict_upsert(
                            specs,
                            idx,
                            *flags,
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
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_replace(
                            specs,
                            idx,
                            *flags,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Remove {
                    path_len,
                    path,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_remove(specs, idx, *flags, path.as_ptr(), *path_len),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::Counter {
                    path_len,
                    path,
                    delta,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_counter(
                            specs,
                            idx,
                            *flags,
                            path.as_ptr(),
                            *path_len,
                            *delta,
                        ),
                        cookie,
                    )?;
                }
                EncodedMutateSpec::ArrayAppend {
                    path_len,
                    path,
                    value_len,
                    value,
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_last(
                            specs,
                            idx,
                            *flags,
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
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_first(
                            specs,
                            idx,
                            *flags,
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
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_array_add_unique(
                            specs,
                            idx,
                            *flags,
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
                    flags,
                } => {
                    verify(
                        lcb_subdocspecs_array_insert(
                            specs,
                            idx,
                            *flags,
                            path.as_ptr(),
                            *path_len,
                            value.as_ptr(),
                            *value_len,
                        ),
                        cookie,
                    )?;
                }
            }
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
                lcb_cmdsubdoc_expiry(command, expiry.as_secs() as u32),
                cookie,
            )?;
        }
        if let Some(access_deleted) = request.options.access_deleted {
            verify(
                lcb_cmdsubdoc_access_deleted(command, if access_deleted { 1 } else { 0 }),
                cookie,
            )?;
        }
        if request.options.preserve_expiry {
            verify(lcb_cmdsubdoc_preserve_expiry(command, 1), cookie)?;
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

    let (body_len, body) = into_cstring(request.payload.unwrap_or_else(|| String::from("")));
    let (content_type_len, content_type) =
        into_cstring(request.content_type.unwrap_or_else(|| String::from("")));

    let service_type = match request.service_type {
        Some(s) => match s {
            ServiceType::Management => lcb_HTTP_TYPE_LCB_HTTP_TYPE_MANAGEMENT,
            ServiceType::KeyValue => {
                panic!("Not supported yet!")
            }
            ServiceType::Views => lcb_HTTP_TYPE_LCB_HTTP_TYPE_VIEW,
            ServiceType::Query => {
                panic!("Not supported yet!")
            }
            ServiceType::Search => lcb_HTTP_TYPE_LCB_HTTP_TYPE_SEARCH,
            ServiceType::Analytics => lcb_HTTP_TYPE_LCB_HTTP_TYPE_ANALYTICS,
        },
        None => lcb_HTTP_TYPE_LCB_HTTP_TYPE_MANAGEMENT,
    };

    let mut command: *mut lcb_CMDHTTP = ptr::null_mut();
    unsafe {
        verify_http(lcb_cmdhttp_create(&mut command, service_type), cookie)?;
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
        .unwrap_or_else(|| Uuid::new_v4().hyphenated().to_string());
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
