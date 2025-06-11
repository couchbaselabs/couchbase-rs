use crate::clients::query_index_mgmt_client::QueryIndexMgmtClient;
use crate::error;
use crate::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllIndexesOptions,
    WatchQueryIndexOptions,
};
use crate::results::query_index_mgmt_results::QueryIndex;
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryIndexManager {
    pub(crate) client: Arc<QueryIndexMgmtClient>,
}

impl QueryIndexManager {
    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        self.client.get_all_indexes(opts.into()).await
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .create_index(index_name.into(), fields.into(), opts.into())
            .await
    }

    pub async fn create_primary_index(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client.create_primary_index(opts.into()).await
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client.drop_index(index_name.into(), opts.into()).await
    }

    pub async fn drop_primary_index(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client.drop_primary_index(opts.into()).await
    }

    pub async fn watch_indexes(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .watch_indexes(index_names.into(), opts.into())
            .await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client.build_deferred_indexes(opts.into()).await
    }
}
