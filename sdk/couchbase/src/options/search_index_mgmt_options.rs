use std::sync::Arc;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetIndexOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetIndexOptions {
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
pub struct UpsertIndexOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl UpsertIndexOptions {
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
pub struct DeleteIndexOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl DeleteIndexOptions {
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
pub struct AnalyzeDocumentOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl AnalyzeDocumentOptions {
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
pub struct GetIndexedDocumentsCountOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl GetIndexedDocumentsCountOptions {
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
pub struct PauseIngestOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl PauseIngestOptions {
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
pub struct ResumeIngestOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl ResumeIngestOptions {
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
pub struct AllowQueryingOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl AllowQueryingOptions {
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
pub struct DisallowQueryingOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl DisallowQueryingOptions {
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
pub struct FreezePlanOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl FreezePlanOptions {
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
pub struct UnfreezePlanOptions {
    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl UnfreezePlanOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
