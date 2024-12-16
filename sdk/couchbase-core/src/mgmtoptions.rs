use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct CreateScopeOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct DeleteScopeOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct CreateCollectionOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default, setter(strip_option))]
    pub max_ttl: Option<i32>,
    #[builder(default, setter(strip_option))]
    pub history_enabled: Option<bool>,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct UpdateCollectionOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default, setter(strip_option))]
    pub max_ttl: Option<i32>,
    #[builder(default, setter(strip_option))]
    pub history_enabled: Option<bool>,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct DeleteCollectionOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,

    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
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

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct EnsureManifestOptions<'a> {
    #[builder(default, setter(strip_option))]
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    #[builder(setter(!into))]
    pub manifest_uid: u64,
}
