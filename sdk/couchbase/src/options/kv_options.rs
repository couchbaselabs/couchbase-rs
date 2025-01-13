use crate::durability_level::DurabilityLevel;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertOptions {
    pub(crate) expiry: Option<Duration>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub(crate) preserve_expiry: Option<bool>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct InsertOptions {
    pub(crate) expiry: Option<Duration>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) expiry: Option<Duration>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) cas: Option<u64>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
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
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetOptions {
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
pub struct ExistsOptions {
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) cas: Option<u64>,
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub(crate) access_deleted: Option<bool>,
}

impl LookupInOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
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
    pub(crate) retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub(crate) expiry: Option<Duration>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) store_semantics: Option<StoreSemantics>,
    pub(crate) access_deleted: Option<bool>,
}

impl MutateInOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
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
}
