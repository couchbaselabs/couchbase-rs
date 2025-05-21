use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::errmapcomponent::ErrMapComponent;
use crate::error::{Error, ErrorKind};
use crate::memdx::error::ErrorKind::{Cancelled, Dispatch, Resource, Server};
use crate::memdx::error::{CancellationErrorKind, ServerError, ServerErrorKind};
use crate::retrybesteffort::controlled_backoff;
use crate::retryfailfast::FailFastRetryStrategy;
use crate::{error, queryx, searchx};
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
        )
    }

    pub fn always_retry(&self) -> bool {
        match self {
            RetryReason::KvInvalidVbucketMap => true,
            RetryReason::KvNotMyVbucket => true,
            RetryReason::KvTemporaryFailure => false,
            RetryReason::KvCollectionOutdated => true,
            RetryReason::KvErrorMapRetryIndicated => false,
            RetryReason::KvLocked => false,
            _ => false,
        }
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
        }
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
        request: &mut RetryInfo,
        reason: RetryReason,
    ) -> Option<Duration> {
        if reason.always_retry() {
            request.add_retry_attempt(reason);
            let backoff = controlled_backoff(request.retry_attempts);

            return Some(backoff);
        }

        let strategy = request.retry_strategy();
        let action = strategy.retry_after(request, &reason).await;

        if let Some(a) = action {
            request.add_retry_attempt(reason);

            return Some(a.duration);
        }

        None
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

#[async_trait]
pub trait RetryStrategy: Debug + Send + Sync {
    async fn retry_after(&self, request: &RetryInfo, reason: &RetryReason) -> Option<RetryAction>;
}

#[derive(Clone, Debug)]
pub struct RetryInfo {
    pub(crate) operation: &'static str,
    pub(crate) is_idempotent: bool,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
    pub(crate) retry_attempts: u32,
    pub(crate) retry_reasons: HashSet<RetryReason>,
    pub(crate) unique_id: Option<String>,
}

impl RetryInfo {
    pub(crate) fn new(
        operation: &'static str,
        is_idempotent: bool,
        retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            operation,
            is_idempotent,
            retry_strategy,
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

    pub fn retry_strategy(&self) -> &Arc<dyn RetryStrategy> {
        &self.retry_strategy
    }

    pub fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    pub fn retry_reasons(&self) -> &HashSet<RetryReason> {
        &self.retry_reasons
    }
}

impl Display for RetryInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ operation: {},  id: {}, is_idempotent: {}, retry_attempts: {}, retry_reasons: {} }}",
            self.operation, self.unique_id.as_ref().unwrap_or(&"".to_string()), self.is_idempotent, self.retry_attempts,
            self.retry_reasons
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub(crate) async fn orchestrate_retries<Fut, Resp>(
    rs: Arc<RetryManager>,
    mut retry_info: RetryInfo,
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
            if let Some(duration) = rs.maybe_retry(&mut retry_info, reason).await {
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
    retry_info: &mut RetryInfo,
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
        ErrorKind::Query(e) => {
            if let queryx::error::ErrorKind::Server(e) = e.kind() {
                match e.kind() {
                    queryx::error::ServerErrorKind::PreparedStatementFailure => {
                        return Some(RetryReason::QueryPreparedStatementFailure);
                    }
                    queryx::error::ServerErrorKind::IndexNotFound => {
                        return Some(RetryReason::QueryIndexNotFound);
                    }
                    _ => {}
                }
            }
        }
        ErrorKind::Search(e) => {
            if let searchx::error::ErrorKind::Server(e) = e.kind() {
                if e.status_code() == 429 {
                    return Some(RetryReason::SearchTooManyRequests);
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
