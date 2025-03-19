use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllScopesOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetAllScopesOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateScopeOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl CreateScopeOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropScopeOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl DropScopeOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl CreateCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl UpdateCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropCollectionOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl DropCollectionOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
