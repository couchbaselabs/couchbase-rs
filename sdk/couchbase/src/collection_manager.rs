use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::error;
use crate::options::collection_mgmt_options::*;
use crate::results::collections_mgmt_results::ScopeSpec;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum MaxExpiryValue {
    Never,
    InheritFromBucket,
    Seconds(Duration),
}

impl From<MaxExpiryValue> for i32 {
    fn from(value: MaxExpiryValue) -> Self {
        match value {
            MaxExpiryValue::Never => 0,
            MaxExpiryValue::InheritFromBucket => -1,
            MaxExpiryValue::Seconds(duration) => duration.as_secs() as i32,
        }
    }
}

impl From<i32> for MaxExpiryValue {
    fn from(value: i32) -> Self {
        match value {
            0 => MaxExpiryValue::Never,
            -1 => MaxExpiryValue::InheritFromBucket,
            _ => MaxExpiryValue::Seconds(Duration::from_secs(value as u64)),
        }
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionSettings {
    pub max_expiry: Option<MaxExpiryValue>,
    pub history: Option<bool>,
}

impl CreateCollectionSettings {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn max_expiry(mut self, max_expiry: MaxExpiryValue) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    pub fn history(mut self, history: bool) -> Self {
        self.history = Some(history);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionSettings {
    pub max_expiry: Option<MaxExpiryValue>,
    pub history: Option<bool>,
}

impl UpdateCollectionSettings {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn max_expiry(mut self, max_expiry: MaxExpiryValue) -> Self {
        self.max_expiry = Some(max_expiry);
        self
    }

    pub fn history(mut self, history: bool) -> Self {
        self.history = Some(history);
        self
    }
}

#[derive(Clone)]
pub struct CollectionManager {
    pub(crate) client: Arc<CollectionsMgmtClient>,
}

impl CollectionManager {
    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        self.client
            .create_scope(
                scope_name,
                opts.into().unwrap_or(CreateScopeOptions::default()),
            )
            .await
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        self.client
            .drop_scope(
                scope_name,
                opts.into().unwrap_or(DropScopeOptions::default()),
            )
            .await
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        self.client
            .create_collection(
                scope_name,
                collection_name,
                settings,
                opts.into().unwrap_or(CreateCollectionOptions::default()),
            )
            .await
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        self.client
            .update_collection(
                scope_name,
                collection_name,
                settings,
                opts.into().unwrap_or(UpdateCollectionOptions::default()),
            )
            .await
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        self.client
            .drop_collection(
                scope_name,
                collection_name,
                opts.into().unwrap_or(DropCollectionOptions::default()),
            )
            .await
    }

    pub async fn get_all_scopes(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        self.client
            .get_all_scopes(opts.into().unwrap_or(GetAllScopesOptions::default()))
            .await
    }
}
