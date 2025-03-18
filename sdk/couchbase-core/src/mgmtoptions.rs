use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx;
use crate::mgmtx::bucket_settings::{BucketSettings, MutableBucketSettings};
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use std::sync::Arc;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) max_ttl: Option<i32>,
    pub(crate) history_enabled: Option<bool>,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) max_ttl: Option<i32>,
    pub(crate) history_enabled: Option<bool>,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
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
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) manifest_uid: u64,
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetAllBucketsOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAllBucketsOptions<'a> {
    pub fn new() -> Self {
        Self {
            on_behalf_of_info: None,
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

impl Default for GetAllBucketsOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> From<&GetAllBucketsOptions<'a>> for mgmtx::options::GetAllBucketsOptions<'a> {
    fn from(opts: &GetAllBucketsOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetBucketOptions<'a> {
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

impl<'a> From<&GetBucketOptions<'a>> for mgmtx::options::GetBucketOptions<'a> {
    fn from(opts: &GetBucketOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) bucket_settings: &'a BucketSettings,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> CreateBucketOptions<'a> {
    pub fn new(bucket_name: &'a str, bucket_settings: &'a BucketSettings) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            bucket_settings,
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

impl<'a> From<&CreateBucketOptions<'a>> for mgmtx::options::CreateBucketOptions<'a> {
    fn from(opts: &CreateBucketOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            bucket_settings: opts.bucket_settings,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UpdateBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) bucket_settings: &'a MutableBucketSettings,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpdateBucketOptions<'a> {
    pub fn new(bucket_name: &'a str, bucket_settings: &'a MutableBucketSettings) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            bucket_settings,
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

impl<'a> From<&UpdateBucketOptions<'a>> for mgmtx::options::UpdateBucketOptions<'a> {
    fn from(opts: &UpdateBucketOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
            bucket_settings: opts.bucket_settings,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteBucketOptions<'a> {
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

impl<'a> From<&DeleteBucketOptions<'a>> for mgmtx::options::DeleteBucketOptions<'a> {
    fn from(opts: &DeleteBucketOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of_info,
            bucket_name: opts.bucket_name,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) bucket_uuid: Option<&'a str>,
    pub(crate) want_missing: bool,
}

impl<'a> EnsureBucketOptions<'a> {
    pub fn new(bucket_name: &'a str, want_missing: bool) -> Self {
        Self {
            on_behalf_of_info: None,
            bucket_name,
            bucket_uuid: None,
            want_missing,
        }
    }

    pub fn bucket_uuid(mut self, bucket_uuid: &'a str) -> Self {
        self.bucket_uuid = Some(bucket_uuid);
        self
    }

    pub fn on_behalf_of_info(mut self, on_behalf_of_info: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of_info = Some(on_behalf_of_info);
        self
    }
}
