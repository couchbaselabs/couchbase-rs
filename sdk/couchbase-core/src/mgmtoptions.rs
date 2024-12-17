use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use std::sync::Arc;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetCollectionManifestOptions<'a> {
    pub fn new(bucket_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&GetCollectionManifestOptions<'a>>
    for mgmtx::options::GetCollectionManifestOptions<'a>
{
    fn from(opts: &GetCollectionManifestOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> CreateScopeOptions<'a> {
    pub fn new(bucket_name: &'a str, scope_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            scope_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&CreateScopeOptions<'a>> for mgmtx::options::CreateScopeOptions<'a> {
    fn from(opts: &CreateScopeOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteScopeOptions<'a> {
    pub fn new(bucket_name: &'a str, scope_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            scope_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&DeleteScopeOptions<'a>> for mgmtx::options::DeleteScopeOptions<'a> {
    fn from(opts: &DeleteScopeOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> CreateCollectionOptions<'a> {
    pub fn new(bucket_name: &'a str, scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            scope_name,
            collection_name,
            max_ttl: None,
            history_enabled: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn max_ttl(mut self, max_ttl: i32) -> Self {
        self.max_ttl = Some(max_ttl);
        self
    }

    pub fn history_enabled(mut self, history_enabled: bool) -> Self {
        self.history_enabled = Some(history_enabled);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&CreateCollectionOptions<'a>> for mgmtx::options::CreateCollectionOptions<'a> {
    fn from(opts: &CreateCollectionOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            max_ttl: opts.max_ttl,
            history_enabled: opts.history_enabled,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpdateCollectionOptions<'a> {
    pub fn new(bucket_name: &'a str, scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            scope_name,
            collection_name,
            max_ttl: None,
            history_enabled: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn max_ttl(mut self, max_ttl: i32) -> Self {
        self.max_ttl = Some(max_ttl);
        self
    }

    pub fn history_enabled(mut self, history_enabled: bool) -> Self {
        self.history_enabled = Some(history_enabled);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&UpdateCollectionOptions<'a>> for mgmtx::options::UpdateCollectionOptions<'a> {
    fn from(opts: &UpdateCollectionOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            max_ttl: opts.max_ttl,
            history_enabled: opts.history_enabled,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,

    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteCollectionOptions<'a> {
    pub fn new(bucket_name: &'a str, scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}

impl<'a> From<&DeleteCollectionOptions<'a>> for mgmtx::options::DeleteCollectionOptions<'a> {
    fn from(opts: &DeleteCollectionOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureManifestOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub manifest_uid: u64,
}

impl<'a> EnsureManifestOptions<'a> {
    pub fn new(bucket_name: &'a str, manifest_uid: u64) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            manifest_uid,
        }
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}
