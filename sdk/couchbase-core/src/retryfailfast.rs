use std::fmt::Debug;

use async_trait::async_trait;

use crate::retry::{RetryAction, RetryInfo, RetryReason, RetryStrategy};

#[derive(Debug, Default)]
pub struct FailFastRetryStrategy {}

#[async_trait]
impl RetryStrategy for FailFastRetryStrategy {
    async fn retry_after(&self, request: &RetryInfo, reason: &RetryReason) -> Option<RetryAction> {
        None
    }
}
