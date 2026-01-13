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

use crate::retry::RetryStrategy;
use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllBucketsOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAllBucketsOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetBucketOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateBucketOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CreateBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateBucketOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UpdateBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropBucketOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl DropBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FlushBucketOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl FlushBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
