use crate::clients::query_client::QueryClient;
use crate::clients::scope_client::ScopeClient;
use crate::collection::Collection;
use crate::error;
use crate::options::query_options::QueryOptions;
use crate::results::query_results::QueryResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());

        Self {
            client,
            query_client,
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
}
