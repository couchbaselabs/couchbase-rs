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
pub struct GetAllQueryIndexesOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAllQueryIndexesOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateQueryIndexOptions {
    pub ignore_if_exists: Option<bool>,
    pub num_replicas: Option<u32>,
    pub deferred: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CreateQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            ignore_if_exists: None,
            num_replicas: None,
            deferred: None,
            retry_strategy: None,
        }
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreatePrimaryQueryIndexOptions {
    pub index_name: Option<String>,
    pub ignore_if_exists: Option<bool>,
    pub num_replicas: Option<u32>,
    pub deferred: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl CreatePrimaryQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            index_name: None,
            ignore_if_exists: None,
            num_replicas: None,
            deferred: None,
            retry_strategy: None,
        }
    }

    pub fn index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropQueryIndexOptions {
    pub ignore_if_not_exists: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl DropQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            ignore_if_not_exists: None,
            retry_strategy: None,
        }
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropPrimaryQueryIndexOptions {
    pub index_name: Option<String>,
    pub ignore_if_not_exists: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl DropPrimaryQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            index_name: None,
            ignore_if_not_exists: None,
            retry_strategy: None,
        }
    }

    pub fn index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct WatchQueryIndexOptions {
    pub watch_primary: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl WatchQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            watch_primary: None,
            retry_strategy: None,
        }
    }

    pub fn watch_primary(mut self, watch_primary: bool) -> Self {
        self.watch_primary = Some(watch_primary);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct BuildQueryIndexOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl BuildQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            retry_strategy: None,
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
