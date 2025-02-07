use crate::clients::query_index_mgmt_client::QueryIndexMgmtClient;
use crate::clients::tracing_client::TracingClient;
use crate::error;
use crate::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllIndexesOptions,
    WatchQueryIndexOptions,
};
use crate::results::query_index_mgmt_results::QueryIndex;
use std::sync::Arc;
use tracing::{instrument, Level};

pub struct CollectionQueryIndexManager {
    pub(crate) client: Arc<QueryIndexMgmtClient>,
    pub(crate) tracing_client: Arc<TracingClient>,
}

impl CollectionQueryIndexManager {
    pub(crate) fn new(client: Arc<QueryIndexMgmtClient>) -> Self {
        let tracing_client = Arc::new(client.tracing_client());
        Self {
            client,
            tracing_client,
        }
    }
    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        self.get_all_indexes_internal(opts).await
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.create_index_internal(index_name, fields, opts).await
    }

    pub async fn create_primary_index(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.create_primary_index_internal(opts).await
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.drop_index_internal(index_name, opts).await
    }

    pub async fn drop_primary_index(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.drop_primary_index_internal(opts).await
    }

    pub async fn watch_indexes(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.watch_indexes_internal(index_names, opts).await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.build_deferred_indexes_internal(opts).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_get_all_indexes",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_get_all_indexes",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn get_all_indexes_internal(
        &self,
        opts: impl Into<Option<GetAllIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        self.tracing_client.record_generic_fields().await;

        self.client.get_all_indexes(opts.into()).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_create_index",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_create_index",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn create_index_internal(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client
            .create_index(index_name.into(), fields.into(), opts.into())
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_create_primary_index",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_create_primary_index",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn create_primary_index_internal(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client.create_primary_index(opts.into()).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_drop_index",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_drop_index",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn drop_index_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client.drop_index(index_name.into(), opts.into()).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_drop_primary_index",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_drop_primary_index",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn drop_primary_index_internal(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client.drop_primary_index(opts.into()).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_watch_indexes",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_watch_indexes",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn watch_indexes_internal(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client
            .watch_indexes(index_names.into(), opts.into())
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_query_build_deferred_indexes",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.operation = "manager_query_build_deferred_indexes",
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.collection = self.client.collection_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn build_deferred_indexes_internal(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.tracing_client.record_generic_fields().await;

        self.client.build_deferred_indexes(opts.into()).await
    }
}
