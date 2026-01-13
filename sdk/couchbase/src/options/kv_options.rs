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
use crate::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub preserve_expiry: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UpsertOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct InsertOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl InsertOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ReplaceOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ReplaceOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetOptions {
    pub expiry: Option<bool>,
    pub projections: Option<Vec<String>>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: bool) -> Self {
        self.expiry = Some(expiry);
        self
    }
    pub fn projections(mut self, projections: Vec<String>) -> Self {
        self.projections = Some(projections);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ExistsOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ExistsOptions {
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
pub struct RemoveOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl RemoveOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAndTouchOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAndTouchOptions {
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
pub struct GetAndLockOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAndLockOptions {
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
pub struct UnlockOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UnlockOptions {
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
pub struct TouchOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl TouchOptions {
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
pub struct LookupInOptions {
    pub access_deleted: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl LookupInOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StoreSemantics {
    Replace,
    Upsert,
    Insert,
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct MutateInOptions {
    pub expiry: Option<Duration>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub store_semantics: Option<StoreSemantics>,
    pub access_deleted: Option<bool>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl MutateInOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn store_semantics(mut self, store_semantics: StoreSemantics) -> Self {
        self.store_semantics = Some(store_semantics);
        self
    }

    // Internal
    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
