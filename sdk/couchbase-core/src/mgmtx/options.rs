use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::bucket_settings::BucketSettings;
use crate::mgmtx::node_target::NodeTarget;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteScopeOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub max_ttl: Option<i32>,
    pub history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteCollectionOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub scope_name: &'a str,
    pub collection_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseClusterConfigOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseBucketConfigOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureManifestPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetAllBucketsOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub bucket_settings: &'a BucketSettings,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
    pub bucket_settings: &'a BucketSettings,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct FlushBucketOptions<'a> {
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureBucketPollOptions<C: Client> {
    pub client: Arc<C>,
    pub targets: Vec<NodeTarget>,
}
