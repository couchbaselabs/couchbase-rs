use crate::clients::analytics_client::AnalyticsClient;
use crate::clients::query_client::QueryClient;
use crate::clients::scope_client::ScopeClient;
use crate::clients::search_client::SearchClient;
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

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    analytics_client: Arc<AnalyticsClient>,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let analytics_client = Arc::new(client.analytics_client());

        Self {
            client,
            query_client,
            search_client,
            analytics_client,
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
        self.query_client.query(statement.into(), opts.into()).await
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_client
            .search(index_name.into(), request, opts.into())
            .await
    }

    pub async fn analytics_query<'a>(
        &self,
        statement: impl AsRef<str>,
        opts: impl Into<Option<AnalyticsOptions>>,
    ) -> error::Result<AnalyticsResult> {
        self.analytics_client
            .query(statement.as_ref(), opts.into())
            .await
    }
}
