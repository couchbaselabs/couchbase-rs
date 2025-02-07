use crate::clients::analytics_client::AnalyticsClient;
use crate::clients::query_client::QueryClient;
use crate::clients::scope_client::ScopeClient;
use crate::clients::search_client::SearchClient;
use crate::clients::tracing_client::TracingClient;
use crate::collection::Collection;
use crate::error;
use crate::options::analytics_options::AnalyticsOptions;
use crate::options::query_options::QueryOptions;
use crate::options::search_options::SearchOptions;
use crate::results::analytics_results::AnalyticsResult;
use crate::results::query_results::QueryResult;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    analytics_client: Arc<AnalyticsClient>,
    tracing_client: TracingClient,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let analytics_client = Arc::new(client.analytics_client());
        let tracing_client = client.tracing_client();

        Self {
            client,
            query_client,
            search_client,
            analytics_client,
            tracing_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn collection(&self, name: impl Into<String>) -> Collection {
        Collection::new(self.client.collection_client(name.into()))
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.query_internal(statement.into(), opts.into().unwrap_or_default())
            .await
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_internal(index_name.into(), request, opts.into().unwrap_or_default())
            .await
    }

    pub async fn analytics_query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<AnalyticsOptions>>,
    ) -> error::Result<AnalyticsResult> {
        self.analytics_query_internal(statement.into(), opts.into().unwrap_or_default())
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "query",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "query",
        db.statement = statement,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn query_internal(
        &self,
        statement: String,
        opts: QueryOptions,
    ) -> error::Result<QueryResult> {
        self.tracing_client.record_generic_fields().await;
        self.query_client.query(statement, opts.into()).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "analytics",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "analytics",
        db.statement = statement,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn analytics_query_internal(
        &self,
        statement: String,
        opts: AnalyticsOptions,
    ) -> error::Result<AnalyticsResult> {
        self.tracing_client.record_generic_fields().await;
        self.analytics_client
            .query(statement.as_ref(), opts.into())
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "search",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "search",
        db.operation = index_name,
        db.name = self.client.bucket_name(),
        db.couchbase.scope = self.client.name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn search_internal(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: SearchOptions,
    ) -> error::Result<SearchResult> {
        self.tracing_client.record_generic_fields().await;
        self.search_client
            .search(index_name, request, opts.into())
            .await
    }
}
