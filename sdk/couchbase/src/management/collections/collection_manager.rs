use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::error;
pub use crate::management::collections::collection_settings::{
    CreateCollectionSettings, UpdateCollectionSettings,
};
use crate::options::collection_mgmt_options::*;
use crate::results::collections_mgmt_results::ScopeSpec;

#[derive(Clone)]
pub struct CollectionManager {
    pub(crate) client: CollectionsMgmtClient,
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
