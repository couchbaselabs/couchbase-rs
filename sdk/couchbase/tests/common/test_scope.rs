use crate::common::test_collection::TestCollection;
use crate::common::test_search_index_manager::TestSearchIndexManager;
use couchbase::error;
use couchbase::options::query_options::QueryOptions;
use couchbase::options::search_options::SearchOptions;
use couchbase::results::query_results::QueryResult;
use couchbase::results::search_results::SearchResult;
use couchbase::scope::Scope;
use couchbase::search::request::SearchRequest;
use std::ops::Deref;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct TestScope {
    inner: Scope,
}

impl Deref for TestScope {
    type Target = Scope;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestScope {
    pub fn new(inner: Scope) -> Self {
        Self { inner }
    }

    pub fn collection(&self, name: impl Into<String>) -> TestCollection {
        TestCollection::new(self.inner.collection(name))
    }

    pub fn search_indexes(&self) -> TestSearchIndexManager {
        TestSearchIndexManager::new(self.inner.search_indexes())
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        timeout(Duration::from_secs(15), self.inner.query(statement, opts))
            .await
            .unwrap()
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        timeout(
            Duration::from_secs(15),
            self.inner.search(index_name, request, opts),
        )
        .await
        .unwrap()
    }
}
