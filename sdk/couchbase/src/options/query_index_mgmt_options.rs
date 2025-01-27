use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllIndexesOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetAllIndexesOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateQueryIndexOptions {
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) num_replicas: Option<u32>,
    pub(crate) deferred: Option<bool>,
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreatePrimaryQueryIndexOptions {
    pub(crate) index_name: Option<String>,
    pub(crate) ignore_if_exists: Option<bool>,
    pub(crate) num_replicas: Option<u32>,
    pub(crate) deferred: Option<bool>,
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropQueryIndexOptions {
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropPrimaryQueryIndexOptions {
    pub(crate) index_name: Option<String>,
    pub(crate) ignore_if_not_exists: Option<bool>,
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct WatchQueryIndexOptions {
    pub(crate) watch_primary: Option<bool>,
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct BuildQueryIndexOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl BuildQueryIndexOptions {
    pub fn new() -> Self {
        Self {
            retry_strategy: None,
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
