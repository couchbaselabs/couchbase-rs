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

//! Options for data structure operations (list, map, set, queue).
//!
//! Each data structure type has a corresponding options struct that can be used to
//! customize behavior such as retry strategies.

use crate::retry::RetryStrategy;
use std::sync::Arc;

/// Options for [`CouchbaseList`](crate::collection_ds::CouchbaseList) operations.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseListOptions {
    /// The retry strategy to use for operations.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CouchbaseListOptions {
    /// Creates a new instance of `CouchbaseListOptions`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`CouchbaseMap`](crate::collection_ds::CouchbaseMap) operations.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseMapOptions {
    /// The retry strategy to use for operations.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CouchbaseMapOptions {
    /// Creates a new instance of `CouchbaseMapOptions`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`CouchbaseSet`](crate::collection_ds::CouchbaseSet) operations.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseSetOptions {
    /// The retry strategy to use for operations.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CouchbaseSetOptions {
    /// Creates a new instance of `CouchbaseSetOptions`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`CouchbaseQueue`](crate::collection_ds::CouchbaseQueue) operations.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CouchbaseQueueOptions {
    /// The retry strategy to use for operations.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CouchbaseQueueOptions {
    /// Creates a new instance of `CouchbaseQueueOptions`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
