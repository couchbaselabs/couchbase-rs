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

use std::fmt::Debug;
use std::time::Duration;

use crate::retry::{RetryAction, RetryReason, RetryRequest, RetryStrategy};

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

impl<Calc> RetryStrategy for BestEffortRetryStrategy<Calc>
where
    Calc: BackoffCalculator,
{
    fn retry_after(&self, request: &RetryRequest, reason: &RetryReason) -> Option<RetryAction> {
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
        let factor = self.backoff_factor.powi(retry_attempts as i32);
        let factor_u128 = factor as u128;

        if u128::MAX / self.min.as_millis() < factor_u128 {
            // If the factor is too large, we cap it to prevent overflow.
            return self.max;
        }

        let val = self.min.as_millis() * factor_u128;

        if val > u64::MAX as u128 {
            // If the value exceeds u64::MAX, we cap it to max.
            return self.max;
        }

        let mut backoff = Duration::from_millis(val as u64);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let calculator = ExponentialBackoffCalculator::new(
            Duration::from_millis(10),
            Duration::from_millis(1000),
            2.0,
        );

        assert_eq!(calculator.backoff(0), Duration::from_millis(10));
        assert_eq!(calculator.backoff(1), Duration::from_millis(20));
        assert_eq!(calculator.backoff(2), Duration::from_millis(40));
        assert_eq!(calculator.backoff(3), Duration::from_millis(80));
        assert_eq!(calculator.backoff(4), Duration::from_millis(160));
        assert_eq!(calculator.backoff(5), Duration::from_millis(320));
        assert_eq!(calculator.backoff(6), Duration::from_millis(640));
        assert_eq!(calculator.backoff(7), Duration::from_millis(1000));
    }

    #[test]
    fn test_exponential_backoff_overflows_u128() {
        let calculator = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        assert_eq!(calculator.backoff(208), Duration::from_millis(1000));
    }

    #[test]
    fn test_exponential_backoff_overflows_u64() {
        let calculator = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        assert_eq!(calculator.backoff(207), Duration::from_millis(1000));
    }
}
