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

//! Retry strategies for Couchbase operations.
//!
//! The SDK automatically retries certain operations when transient failures occur.
//! You can customize the retry behavior by providing a custom [`RetryStrategy`] implementation,
//! or use the built-in [`BestEffortRetryStrategy`] with configurable backoff.
//!
//! # Default Behavior
//!
//! By default, the SDK uses a [`BestEffortRetryStrategy`] with exponential backoff.
//! You can override the default strategy at the cluster level via
//! [`ClusterOptions::default_retry_strategy`](crate::options::cluster_options::ClusterOptions::default_retry_strategy),
//! or per-operation via the `retry_strategy` field on any options struct.
//!
//! # Example
//!
//! ```rust
//! use couchbase::retry::BestEffortRetryStrategy;
//! use std::sync::Arc;
//!
//! let strategy = Arc::new(BestEffortRetryStrategy::default());
//! ```
pub use couchbase_core::retry::{RetryAction, RetryReason, RetryRequest, RetryStrategy};
pub use couchbase_core::retrybesteffort::{
    BackoffCalculator, BestEffortRetryStrategy, ExponentialBackoffCalculator,
};
