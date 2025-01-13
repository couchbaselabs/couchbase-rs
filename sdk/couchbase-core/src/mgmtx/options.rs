use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::node_target::NodeTarget;
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetCollectionManifestOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateScopeOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteScopeOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct CreateCollectionOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) max_ttl: Option<i32>,
    pub(crate) history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct UpdateCollectionOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) max_ttl: Option<i32>,
    pub(crate) history_enabled: Option<bool>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeleteCollectionOptions<'a> {
    pub(crate) on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub(crate) bucket_name: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseClusterConfigOptions {
    pub(crate) on_behalf_of_info: Option<OnBehalfOfInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct GetTerseBucketConfigOptions {
    pub(crate) on_behalf_of_info: Option<OnBehalfOfInfo>,
    pub(crate) bucket_name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureManifestPollOptions<C: Client> {
    pub(crate) client: Arc<C>,
    pub(crate) targets: Vec<NodeTarget>,
}
