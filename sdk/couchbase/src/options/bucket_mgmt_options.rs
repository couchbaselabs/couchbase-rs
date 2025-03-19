use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllBucketsOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetAllBucketsOptions {
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
pub struct GetBucketOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetBucketOptions {
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
pub struct CreateBucketOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl CreateBucketOptions {
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
pub struct UpdateBucketOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl UpdateBucketOptions {
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
pub struct DeleteBucketOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl DeleteBucketOptions {
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
pub struct FlushBucketOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl FlushBucketOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
