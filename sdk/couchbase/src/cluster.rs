use crate::bucket::Bucket;
use crate::clients::analytics_client::AnalyticsClient;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::query_client::QueryClient;
use crate::clients::search_client::SearchClient;
use crate::clients::tracing_client::TracingClient;
use crate::error;
use crate::options::analytics_options::AnalyticsOptions;
use crate::options::cluster_options::ClusterOptions;
use crate::options::query_options::QueryOptions;
use crate::options::search_options::SearchOptions;
use crate::results::analytics_results::AnalyticsResult;
use crate::results::query_results::QueryResult;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use std::sync::Arc;
use tracing::instrument;
use tracing::Level;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    analytics_client: Arc<AnalyticsClient>,
    tracing_client: Arc<TracingClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let query_client = Arc::new(client.query_client()?);
        let search_client = Arc::new(client.search_client()?);
        let analytics_client = Arc::new(client.analytics_client()?);
        let tracing_client = Arc::new(client.tracing_client()?);

        Ok(Cluster {
            client,
            query_client,
            search_client,
            analytics_client,
            tracing_client,
        })
    }

    pub fn bucket(&self, name: impl Into<String>) -> Bucket {
        let bucket_client = self.client.bucket_client(name.into());

        Bucket::new(bucket_client)
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.query_internal(statement.into(), opts).await
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_internal(index_name.into(), request, opts).await
    }

    pub async fn analytics_query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<AnalyticsOptions>>,
    ) -> error::Result<AnalyticsResult> {
        self.analytics_query_internal(statement.into(), opts).await
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
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn query_internal(
        &self,
        statement: String,
        opts: impl Into<Option<QueryOptions>>,
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
        db.statement = statement.as_ref(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn analytics_query_internal(
        &self,
        statement: impl AsRef<str>,
        opts: impl Into<Option<AnalyticsOptions>>,
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
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn search_internal(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.tracing_client.record_generic_fields().await;
        self.search_client
            .search(index_name, request, opts.into())
            .await
    }
}
