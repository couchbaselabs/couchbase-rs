/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::errmapcomponent::ErrMapComponent;
use crate::error::{Error, ErrorKind};
use crate::memdx::error::ErrorKind::{Cancelled, Dispatch, Resource, Server};
use crate::memdx::error::{CancellationErrorKind, ServerError, ServerErrorKind};
use crate::retryfailfast::FailFastRetryStrategy;
use crate::{analyticsx, error, httpx, mgmtx, queryx, searchx};
use async_trait::async_trait;
use log::{debug, info};
use tokio::time::sleep;

lazy_static! {
    pub(crate) static ref DEFAULT_RETRY_STRATEGY: Arc<dyn RetryStrategy> =
        Arc::new(FailFastRetryStrategy::default());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RetryReason {
    KvNotMyVbucket,
    KvInvalidVbucketMap,
    KvTemporaryFailure,
    KvCollectionOutdated,
    KvErrorMapRetryIndicated,
    KvLocked,
    KvSyncWriteInProgress,
    KvSyncWriteRecommitInProgress,
    ServiceNotAvailable,
    SocketClosedWhileInFlight,
    SocketNotAvailable,
    QueryPreparedStatementFailure,
    QueryIndexNotFound,
    SearchTooManyRequests,
    HttpSendRequestFailed,
    NotReady,
}

impl RetryReason {
    pub fn allows_non_idempotent_retry(&self) -> bool {
        matches!(
            self,
            RetryReason::KvInvalidVbucketMap
                | RetryReason::KvNotMyVbucket
                | RetryReason::KvTemporaryFailure
                | RetryReason::KvCollectionOutdated
                | RetryReason::KvErrorMapRetryIndicated
                | RetryReason::KvLocked
                | RetryReason::ServiceNotAvailable
                | RetryReason::SocketNotAvailable
                | RetryReason::KvSyncWriteInProgress
                | RetryReason::KvSyncWriteRecommitInProgress
                | RetryReason::QueryPreparedStatementFailure
                | RetryReason::QueryIndexNotFound
                | RetryReason::SearchTooManyRequests
                | RetryReason::HttpSendRequestFailed
                | RetryReason::NotReady
        )
    }

    pub fn always_retry(&self) -> bool {
        matches!(
            self,
            RetryReason::KvInvalidVbucketMap
                | RetryReason::KvNotMyVbucket
                | RetryReason::KvCollectionOutdated
        )
    }
}

impl Display for RetryReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryReason::KvNotMyVbucket => write!(f, "KV_NOT_MY_VBUCKET"),
            RetryReason::KvInvalidVbucketMap => write!(f, "KV_INVALID_VBUCKET_MAP"),
            RetryReason::KvTemporaryFailure => write!(f, "KV_TEMPORARY_FAILURE"),
            RetryReason::KvCollectionOutdated => write!(f, "KV_COLLECTION_OUTDATED"),
            RetryReason::KvErrorMapRetryIndicated => write!(f, "KV_ERROR_MAP_RETRY_INDICATED"),
            RetryReason::KvLocked => write!(f, "KV_LOCKED"),
            RetryReason::ServiceNotAvailable => write!(f, "SERVICE_NOT_AVAILABLE"),
            RetryReason::SocketClosedWhileInFlight => write!(f, "SOCKET_CLOSED_WHILE_IN_FLIGHT"),
            RetryReason::SocketNotAvailable => write!(f, "SOCKET_NOT_AVAILABLE"),
            RetryReason::KvSyncWriteInProgress => write!(f, "KV_SYNC_WRITE_IN_PROGRESS"),
            RetryReason::KvSyncWriteRecommitInProgress => {
                write!(f, "KV_SYNC_WRITE_RECOMMIT_IN_PROGRESS")
            }
            RetryReason::QueryPreparedStatementFailure => {
                write!(f, "QUERY_PREPARED_STATEMENT_FAILURE")
            }
            RetryReason::QueryIndexNotFound => write!(f, "QUERY_INDEX_NOT_FOUND"),
            RetryReason::SearchTooManyRequests => write!(f, "SEARCH_TOO_MANY_REQUESTS"),
            RetryReason::NotReady => write!(f, "NOT_READY"),
            RetryReason::HttpSendRequestFailed => write!(f, "HTTP_SEND_REQUEST_FAILED"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RetryAction {
    pub duration: Duration,
}

impl RetryAction {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

pub trait RetryStrategy: Debug + Send + Sync {
    fn retry_after(&self, request: &RetryRequest, reason: &RetryReason) -> Option<RetryAction>;
}

#[derive(Clone, Debug)]
pub struct RetryRequest {
    pub(crate) operation: &'static str,
    pub is_idempotent: bool,
    pub retry_attempts: u32,
    pub retry_reasons: HashSet<RetryReason>,
    pub(crate) unique_id: Option<String>,
}

impl RetryRequest {
    pub(crate) fn new(operation: &'static str, is_idempotent: bool) -> Self {
        Self {
            operation,
            is_idempotent,
            retry_attempts: 0,
            retry_reasons: Default::default(),
            unique_id: None,
        }
    }

    pub(crate) fn add_retry_attempt(&mut self, reason: RetryReason) {
        self.retry_attempts += 1;
        self.retry_reasons.insert(reason);
    }

    pub fn is_idempotent(&self) -> bool {
        self.is_idempotent
    }

    pub fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    pub fn retry_reasons(&self) -> &HashSet<RetryReason> {
        &self.retry_reasons
    }
}

impl Display for RetryRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ operation: {}, id: {}, is_idempotent: {}, retry_attempts: {}, retry_reasons: {} }}",
            self.operation,
            self.unique_id.as_ref().unwrap_or(&"".to_string()),
            self.is_idempotent,
            self.retry_attempts,
            self.retry_reasons
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub struct RetryManager {
    err_map_component: Arc<ErrMapComponent>,
}

impl RetryManager {
    pub fn new(err_map_component: Arc<ErrMapComponent>) -> Self {
        Self { err_map_component }
    }

    pub async fn maybe_retry(
        &self,
        strategy: Arc<dyn RetryStrategy>,
        request: &mut RetryRequest,
        reason: RetryReason,
    ) -> Option<Duration> {
        if reason.always_retry() {
            request.add_retry_attempt(reason);
            let backoff = controlled_backoff(request.retry_attempts);

            return Some(backoff);
        }

        let action = strategy.retry_after(request, &reason);

        if let Some(a) = action {
            request.add_retry_attempt(reason);

            return Some(a.duration);
        }

        None
    }
}

pub(crate) async fn orchestrate_retries<Fut, Resp>(
    rs: Arc<RetryManager>,
    strategy: Arc<dyn RetryStrategy>,
    mut retry_info: RetryRequest,
    operation: impl Fn() -> Fut + Send + Sync,
) -> error::Result<Resp>
where
    Fut: Future<Output = error::Result<Resp>> + Send,
    Resp: Send,
{
    loop {
        let mut err = match operation().await {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => e,
        };

        if let Some(reason) = error_to_retry_reason(&rs, &mut retry_info, &err) {
            if let Some(duration) = rs
                .maybe_retry(strategy.clone(), &mut retry_info, reason)
                .await
            {
                debug!(
                    "Retrying {} after {:?} due to {}",
                    &retry_info, duration, reason
                );
                sleep(duration).await;
                continue;
            }
        }

        if retry_info.retry_attempts > 0 {
            // If we aren't retrying then attach any retry info that we have.
            err.set_retry_info(retry_info);
        }

        return Err(err);
    }
}

pub(crate) fn error_to_retry_reason(
    rs: &Arc<RetryManager>,
    retry_info: &mut RetryRequest,
    err: &Error,
) -> Option<RetryReason> {
    match err.kind() {
        ErrorKind::Memdx(err) => {
            retry_info.unique_id = err.has_opaque().map(|o| o.to_string());

            match err.kind() {
                Server(e) => return server_error_to_retry_reason(rs, e),
                Resource(e) => return server_error_to_retry_reason(rs, e.cause()),
                Cancelled(e) => {
                    if e == &CancellationErrorKind::ClosedInFlight {
                        return Some(RetryReason::SocketClosedWhileInFlight);
                    }
                }
                Dispatch { .. } => return Some(RetryReason::SocketNotAvailable),
                _ => {}
            }
        }
        ErrorKind::NoVbucketMap => {
            return Some(RetryReason::KvInvalidVbucketMap);
        }
        ErrorKind::ServiceNotAvailable { .. } => {
            return Some(RetryReason::ServiceNotAvailable);
        }
        ErrorKind::Query(e) => match e.kind() {
            queryx::error::ErrorKind::Server(e) => match e.kind() {
                queryx::error::ServerErrorKind::PreparedStatementFailure => {
                    return Some(RetryReason::QueryPreparedStatementFailure);
                }
                queryx::error::ServerErrorKind::IndexNotFound => {
                    return Some(RetryReason::QueryIndexNotFound);
                }
                _ => {}
            },
            queryx::error::ErrorKind::Http { error, .. } => {
                if let httpx::error::ErrorKind::SendRequest(_) = error.kind() {
                    return Some(RetryReason::HttpSendRequestFailed);
                }
            }
            _ => {}
        },
        ErrorKind::Search(e) => match e.kind() {
            searchx::error::ErrorKind::Server(e) => {
                if e.status_code() == 429 {
                    return Some(RetryReason::SearchTooManyRequests);
                }
            }
            searchx::error::ErrorKind::Http { error, .. } => {
                if let httpx::error::ErrorKind::SendRequest(_) = error.kind() {
                    return Some(RetryReason::HttpSendRequestFailed);
                }
            }
            _ => {}
        },
        ErrorKind::Analytics(e) => {
            if let analyticsx::error::ErrorKind::Http { error, .. } = e.kind() {
                if let httpx::error::ErrorKind::SendRequest(_) = error.kind() {
                    return Some(RetryReason::HttpSendRequestFailed);
                }
            }
        }
        ErrorKind::Mgmt(e) => {
            if let mgmtx::error::ErrorKind::Http(error) = e.kind() {
                if let httpx::error::ErrorKind::SendRequest(_) = error.kind() {
                    return Some(RetryReason::HttpSendRequestFailed);
                }
            }
        }
        _ => {}
    }

    None
}

fn server_error_to_retry_reason(rs: &Arc<RetryManager>, e: &ServerError) -> Option<RetryReason> {
    match e.kind() {
        ServerErrorKind::NotMyVbucket => {
            return Some(RetryReason::KvNotMyVbucket);
        }
        ServerErrorKind::TmpFail => {
            return Some(RetryReason::KvTemporaryFailure);
        }
        ServerErrorKind::UnknownCollectionID => {
            return Some(RetryReason::KvCollectionOutdated);
        }
        ServerErrorKind::UnknownCollectionName => {
            return Some(RetryReason::KvCollectionOutdated);
        }
        ServerErrorKind::UnknownScopeName => {
            return Some(RetryReason::KvCollectionOutdated);
        }
        ServerErrorKind::Locked => {
            return Some(RetryReason::KvLocked);
        }
        ServerErrorKind::SyncWriteInProgress => {
            return Some(RetryReason::KvSyncWriteInProgress);
        }
        ServerErrorKind::SyncWriteRecommitInProgress => {
            return Some(RetryReason::KvSyncWriteRecommitInProgress);
        }
        ServerErrorKind::UnknownStatus { status } => {
            if rs.err_map_component.should_retry(status) {
                return Some(RetryReason::KvErrorMapRetryIndicated);
            }
        }
        _ => {}
    }

    None
}

pub(crate) fn controlled_backoff(retry_attempts: u32) -> Duration {
    match retry_attempts {
        0 => Duration::from_millis(1),
        1 => Duration::from_millis(10),
        2 => Duration::from_millis(50),
        3 => Duration::from_millis(100),
        4 => Duration::from_millis(500),
        _ => Duration::from_millis(1000),
    }
}
