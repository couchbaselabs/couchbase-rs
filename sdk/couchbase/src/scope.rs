use crate::clients::query_client::QueryClient;
use crate::clients::scope_client::ScopeClient;
use crate::clients::search_client::SearchClient;
use crate::clients::search_index_mgmt_client::SearchIndexMgmtClient;
use crate::collection::Collection;
use crate::error;
use crate::management::search::search_index_manager::SearchIndexManager;
use crate::options::query_options::QueryOptions;
use crate::options::search_options::SearchOptions;
use crate::results::query_results::QueryResult;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use std::sync::Arc;

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    search_index_client: Arc<SearchIndexMgmtClient>,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let search_index_client = Arc::new(client.search_index_management_client());

        Self {
            client,
            query_client,
            search_client,
            search_index_client,
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

    pub fn search_indexes(&self) -> SearchIndexManager {
        SearchIndexManager {
            client: self.search_index_client.clone(),
        }
    }
}
