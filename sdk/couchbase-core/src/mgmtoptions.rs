use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx;
use crate::mgmtx::bucket_settings::BucketSettings;
use crate::mgmtx::user::{Group, User};
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
    pub(crate) bucket_settings: &'a BucketSettings,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpdateBucketOptions<'a> {
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
pub struct FlushBucketOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,

    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> FlushBucketOptions<'a> {
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

impl<'a> From<&FlushBucketOptions<'a>> for mgmtx::options::FlushBucketOptions<'a> {
    fn from(opts: &FlushBucketOptions<'a>) -> Self {
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetUserOptions<'a> {
    pub username: &'a str,
    pub auth_domain: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetUserOptions<'a> {
    pub fn new(username: &'a str, auth_domain: &'a str) -> Self {
        Self {
            username,
            auth_domain,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetUserOptions<'a>> for mgmtx::options::GetUserOptions<'a> {
    fn from(opts: &GetUserOptions<'a>) -> Self {
        Self {
            username: opts.username,
            auth_domain: opts.auth_domain,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetAllUsersOptions<'a> {
    pub auth_domain: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAllUsersOptions<'a> {
    pub fn new(auth_domain: &'a str) -> Self {
        Self {
            auth_domain,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetAllUsersOptions<'a>> for mgmtx::options::GetAllUsersOptions<'a> {
    fn from(opts: &GetAllUsersOptions<'a>) -> Self {
        Self {
            auth_domain: opts.auth_domain,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UpsertUserOptions<'a> {
    pub user: &'a User,
    pub auth_domain: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpsertUserOptions<'a> {
    pub fn new(user: &'a User, auth_domain: &'a str) -> Self {
        Self {
            user,
            auth_domain,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&UpsertUserOptions<'a>> for mgmtx::options::UpsertUserOptions<'a> {
    fn from(opts: &UpsertUserOptions<'a>) -> Self {
        Self {
            user: opts.user,
            auth_domain: opts.auth_domain,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteUserOptions<'a> {
    pub username: &'a str,
    pub auth_domain: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteUserOptions<'a> {
    pub fn new(username: &'a str, auth_domain: &'a str) -> Self {
        Self {
            username,
            auth_domain,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&DeleteUserOptions<'a>> for mgmtx::options::DeleteUserOptions<'a> {
    fn from(opts: &DeleteUserOptions<'a>) -> Self {
        Self {
            username: opts.username,
            auth_domain: opts.auth_domain,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetRolesOptions<'a> {
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for GetRolesOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> GetRolesOptions<'a> {
    pub fn new() -> Self {
        Self {
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetRolesOptions<'a>> for mgmtx::options::GetRolesOptions<'a> {
    fn from(opts: &GetRolesOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetGroupOptions<'a> {
    pub group_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetGroupOptions<'a> {
    pub fn new(group_name: &'a str) -> Self {
        Self {
            group_name,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetGroupOptions<'a>> for mgmtx::options::GetGroupOptions<'a> {
    fn from(opts: &GetGroupOptions<'a>) -> Self {
        Self {
            group_name: opts.group_name,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetAllGroupsOptions<'a> {
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for GetAllGroupsOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> GetAllGroupsOptions<'a> {
    pub fn new() -> Self {
        Self {
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetAllGroupsOptions<'a>> for mgmtx::options::GetAllGroupsOptions<'a> {
    fn from(opts: &GetAllGroupsOptions<'a>) -> Self {
        Self {
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UpsertGroupOptions<'a> {
    pub group: &'a Group,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpsertGroupOptions<'a> {
    pub fn new(group: &'a Group) -> Self {
        Self {
            group,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&UpsertGroupOptions<'a>> for mgmtx::options::UpsertGroupOptions<'a> {
    fn from(opts: &UpsertGroupOptions<'a>) -> Self {
        Self {
            group: opts.group,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteGroupOptions<'a> {
    pub group_name: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteGroupOptions<'a> {
    pub fn new(group_name: &'a str) -> Self {
        Self {
            group_name,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&DeleteGroupOptions<'a>> for mgmtx::options::DeleteGroupOptions<'a> {
    fn from(opts: &DeleteGroupOptions<'a>) -> Self {
        Self {
            group_name: opts.group_name,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChangePasswordOptions<'a> {
    pub new_password: &'a str,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> ChangePasswordOptions<'a> {
    pub fn new(new_password: &'a str) -> Self {
        Self {
            new_password,
            on_behalf_of: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&ChangePasswordOptions<'a>> for mgmtx::options::ChangePasswordOptions<'a> {
    fn from(opts: &ChangePasswordOptions<'a>) -> Self {
        Self {
            new_password: opts.new_password,
            on_behalf_of_info: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureUserOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) username: &'a str,
    pub(crate) auth_domain: &'a str,
    pub(crate) want_missing: bool,
}

impl<'a> EnsureUserOptions<'a> {
    pub fn new(username: &'a str, auth_domain: &'a str, want_missing: bool) -> Self {
        Self {
            on_behalf_of_info: None,
            username,
            auth_domain,
            want_missing,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureGroupOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) group_name: &'a str,
    pub(crate) want_missing: bool,
}

impl<'a> EnsureGroupOptions<'a> {
    pub fn new(group_name: &'a str, want_missing: bool) -> Self {
        Self {
            on_behalf_of_info: None,
            group_name,
            want_missing,
        }
    }
}
