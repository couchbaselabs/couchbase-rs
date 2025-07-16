use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use crate::retry::{RetryAction, RetryInfo, RetryReason, RetryStrategy};

#[derive(Debug, Clone)]
pub struct BestEffortRetryStrategy<Calc> {
    backoff_calc: Calc,
}

impl<Calc> BestEffortRetryStrategy<Calc>
where
    Calc: BackoffCalculator,
{
    pub fn new(calc: Calc) -> Self {
        Self { backoff_calc: calc }
    }
}

impl Default for BestEffortRetryStrategy<ExponentialBackoffCalculator> {
    fn default() -> Self {
        Self::new(ExponentialBackoffCalculator::default())
    }
}

#[async_trait]
impl<Calc> RetryStrategy for BestEffortRetryStrategy<Calc>
where
    Calc: BackoffCalculator,
{
    async fn retry_after(&self, request: &RetryInfo, reason: &RetryReason) -> Option<RetryAction> {
        if request.is_idempotent() || reason.allows_non_idempotent_retry() {
            Some(RetryAction::new(
                self.backoff_calc.backoff(request.retry_attempts()),
            ))
        } else {
            None
        }
    }
}

pub trait BackoffCalculator: Debug + Send + Sync {
    fn backoff(&self, retry_attempts: u32) -> Duration;
}

#[derive(Clone, Debug)]
pub struct ExponentialBackoffCalculator {
    min: Duration,
    max: Duration,
    backoff_factor: f64,
}

impl ExponentialBackoffCalculator {
    pub fn new(min: Duration, max: Duration, backoff_factor: f64) -> Self {
        Self {
            min,
            max,
            backoff_factor,
        }
    }
}

impl BackoffCalculator for ExponentialBackoffCalculator {
    fn backoff(&self, retry_attempts: u32) -> Duration {
        let mut backoff = Duration::from_millis(
            (self.min.as_millis() * self.backoff_factor.powf(retry_attempts as f64) as u128) as u64,
        );

        if backoff > self.max {
            backoff = self.max;
        }
        if backoff < self.min {
            backoff = self.min
        }

        backoff
    }
}

impl Default for ExponentialBackoffCalculator {
    fn default() -> Self {
        Self {
            min: Duration::from_millis(1),
            max: Duration::from_millis(1000),
            backoff_factor: 2.0,
        }
    }
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
