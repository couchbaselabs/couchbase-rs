use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::clients::tracing_client::TracingClient;
use crate::error;
use crate::options::collections_mgmt_options::*;
use crate::results::collections_mgmt_results::ScopeSpec;
use std::sync::Arc;
use std::time::Duration;
use tracing::{instrument, Level};

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
    pub(crate) tracing_client: Arc<TracingClient>,
}

impl CollectionManager {
    pub(crate) fn new(client: Arc<CollectionsMgmtClient>) -> Self {
        let tracing_client = Arc::new(client.tracing_client());
        Self {
            client,
            tracing_client,
        }
    }
    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        self.create_scope_internal(scope_name.into(), opts).await
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        self.drop_scope_internal(scope_name.into(), opts).await
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        self.create_collection_internal(scope_name.into(), collection_name.into(), settings, opts)
            .await
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        self.update_collection_internal(scope_name.into(), collection_name.into(), settings, opts)
            .await
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        self.drop_collection_internal(scope_name.into(), collection_name.into(), opts)
            .await
    }

    pub async fn get_all_scopes(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        self.get_all_scopes_internal(opts).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_create_scope",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = scope_name,
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn create_scope_internal(
        &self,
        scope_name: String,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        let path = format!(
            "POST /pools/default/buckets/{}/scopes",
            self.client.bucket_name()
        );
        self.tracing_client.record_mgmt_fields(&path).await;
        self.client
            .create_scope(
                scope_name,
                opts.into().unwrap_or(CreateScopeOptions::default()),
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_drop_scope",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = scope_name,
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn drop_scope_internal(
        &self,
        scope_name: String,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        let path = format!(
            "DELETE /pools/default/buckets/{}/scopes/{}",
            self.client.bucket_name(),
            scope_name
        );
        self.tracing_client.record_mgmt_fields(&path).await;
        self.client
            .drop_scope(
                scope_name,
                opts.into().unwrap_or(DropScopeOptions::default()),
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_create_collection",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = scope_name,
        db.couchbase.collection = collection_name,
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn create_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        settings: CreateCollectionSettings,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        let path = format!(
            "POST /pools/default/buckets/{}/scopes/{}/collections",
            self.client.bucket_name(),
            scope_name
        );
        self.tracing_client.record_mgmt_fields(&path).await;
        self.client
            .create_collection(
                scope_name,
                collection_name,
                settings,
                opts.into().unwrap_or(CreateCollectionOptions::default()),
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_update_collection",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = scope_name,
        db.couchbase.collection = collection_name,
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn update_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        let path = format!(
            "PATCH /pools/default/buckets/{}/scopes/{}/collections/{}",
            self.client.bucket_name(),
            scope_name,
            collection_name
        );
        self.tracing_client.record_mgmt_fields(&path).await;

        self.client
            .update_collection(
                scope_name,
                collection_name,
                settings,
                opts.into().unwrap_or(UpdateCollectionOptions::default()),
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_drop_collection",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = scope_name,
        db.couchbase.collection = collection_name,
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn drop_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        let path = format!(
            "DELETE /pools/default/buckets/{}/scopes/{}/collections/{}",
            self.client.bucket_name(),
            scope_name,
            collection_name
        );
        self.tracing_client.record_mgmt_fields(&path).await;

        self.client
            .drop_collection(
                scope_name,
                collection_name,
                opts.into().unwrap_or(DropCollectionOptions::default()),
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "manager_collections_get_all_scopes",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "management",
        db.operation,
        db.name = self.client.bucket_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn get_all_scopes_internal(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        let path = format!(
            "GET /pools/default/buckets/{}/scopes",
            self.client.bucket_name()
        );
        self.tracing_client.record_mgmt_fields(&path).await;

        self.client
            .get_all_scopes(opts.into().unwrap_or(GetAllScopesOptions::default()))
            .await
    }
}
