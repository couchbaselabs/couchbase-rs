use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use tokio::time::sleep;

use crate::error;
use crate::error::{Error, ErrorKind};
use crate::memdx::error::ServerErrorKind;
use crate::retrybesteffort::controlled_backoff;
use crate::retryfailfast::FailFastRetryStrategy;

lazy_static! {
    pub(crate) static ref DEFAULT_RETRY_STRATEGY: Arc<dyn RetryStrategy> =
        Arc::new(FailFastRetryStrategy::default());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RetryReason {
    NotMyVbucket,
    InvalidVbucketMap,
    TempFail,
    KvCollectionOutdated,
    Unknown,
}

impl RetryReason {
    pub fn allows_non_idempotent_retry(&self) -> bool {
        match self {
            RetryReason::InvalidVbucketMap => true,
            RetryReason::NotMyVbucket => true,
            RetryReason::TempFail => true,
            RetryReason::KvCollectionOutdated => true,
            RetryReason::Unknown => false,
            _ => false,
        }
    }

    pub fn always_retry(&self) -> bool {
        match self {
            RetryReason::InvalidVbucketMap => true,
            RetryReason::NotMyVbucket => true,
            RetryReason::TempFail => false,
            RetryReason::KvCollectionOutdated => true,
            RetryReason::Unknown => false,
            _ => false,
        }
    }
}

#[derive(Default)]
pub struct RetryManager {}

impl RetryManager {
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
    pub(crate) is_idempotent: bool,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
    pub(crate) retry_attempts: u32,
    pub(crate) retry_reasons: HashSet<RetryReason>,
}

impl RetryInfo {
    pub(crate) fn new(is_idempotent: bool, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        Self {
            is_idempotent,
            retry_strategy,
            retry_attempts: 0,
            retry_reasons: Default::default(),
        }
    }

    pub(crate) fn add_retry_attempt(&mut self, reason: RetryReason) {
        self.retry_attempts += 1;
        tracing::Span::current().record("db.couchbase.retries", self.retry_attempts);
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
        let err = match operation().await {
            Ok(r) => {
                return Ok(r);
            }
            Err(e) => e,
        };

        if let Some(reason) = error_to_retry_reason(&err) {
            if let Some(duration) = rs.maybe_retry(&mut retry_info, reason).await {
                debug!(
                    "Retrying operation after {:?} due to {:?}",
                    duration, reason
                );
                sleep(duration).await;
                continue;
            }
        }

        return Err(err);
    }
}

pub(crate) fn error_to_retry_reason(err: &Error) -> Option<RetryReason> {
    match err.kind.as_ref() {
        ErrorKind::Memdx { source, .. } => {
            if source.is_server_error_kind(ServerErrorKind::NotMyVbucket) {
                return Some(RetryReason::NotMyVbucket);
            }
            if source.is_server_error_kind(ServerErrorKind::TmpFail) {
                return Some(RetryReason::TempFail);
            }
            if source.is_server_error_kind(ServerErrorKind::UnknownCollectionID)
                || source.is_server_error_kind(ServerErrorKind::UnknownCollectionName)
            {
                return Some(RetryReason::KvCollectionOutdated);
            }
        }
        ErrorKind::InvalidVbucketMap => {
            return Some(RetryReason::InvalidVbucketMap);
        }
        _ => {}
    }

    None
}
