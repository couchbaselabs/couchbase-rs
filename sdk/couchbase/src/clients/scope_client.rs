use crate::clients::collection_client::{
    CollectionClient, CollectionClientBackend, Couchbase2CollectionClient,
    CouchbaseCollectionClient,
};
use crate::clients::query_client::{
    CouchbaseQueryClient, QueryClient, QueryClientBackend, QueryKeyspace,
};
use crate::clients::search_client::{
    CouchbaseSearchClient, SearchClient, SearchClientBackend, SearchKeyspace,
};
use couchbase_core::agent::Agent;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ScopeClient {
    backend: ScopeClientBackend,
}

impl ScopeClient {
    pub fn new(scope_client_backend: ScopeClientBackend) -> Self {
        Self {
            backend: scope_client_backend,
        }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => client.name(),
            ScopeClientBackend::Couchbase2ScopeBackend(client) => client.name(),
        }
    }

    pub fn query_client(&self) -> QueryClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(backend) => {
                let query_client = backend.query_client();

                QueryClient::new(QueryClientBackend::CouchbaseQueryClientBackend(
                    query_client,
                ))
            }
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn search_client(&self) -> SearchClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(backend) => {
                let search_client = backend.search_client();

                SearchClient::new(SearchClientBackend::CouchbaseSearchClientBackend(
                    search_client,
                ))
            }
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn collection_client(&self, name: String) -> CollectionClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => client.collection(name),
            ScopeClientBackend::Couchbase2ScopeBackend(client) => client.collection(name),
        }
    }
}

#[derive(Clone)]
pub(crate) enum ScopeClientBackend {
    CouchbaseScopeBackend(CouchbaseScopeClient),
    Couchbase2ScopeBackend(Couchbase2ScopeClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseScopeClient {
    agent: Agent,
    bucket_name: String,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseScopeClient {
    pub fn new(
        agent: Agent,
        bucket_name: String,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent,
            bucket_name,
            name,
            default_retry_strategy,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collection(&self, name: String) -> CollectionClient {
        CollectionClient::new(CollectionClientBackend::CouchbaseCollectionBackend(
            CouchbaseCollectionClient::new(
                self.agent.clone(),
                self.bucket_name().to_string(),
                self.name().to_string(),
                name,
                self.default_retry_strategy.clone(),
            ),
        ))
    }

    pub fn query_client(&self) -> CouchbaseQueryClient {
        CouchbaseQueryClient::with_keyspace(
            self.agent.clone(),
            QueryKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.name().to_string(),
            },
        )
    }

    pub fn search_client(&self) -> CouchbaseSearchClient {
        CouchbaseSearchClient::with_keyspace(
            self.agent.clone(),
            SearchKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.name().to_string(),
            },
        )
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2ScopeClient {
    bucket_name: String,
    name: String,
}

impl Couchbase2ScopeClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collection(&self, _name: String) -> CollectionClient {
        CollectionClient::new(CollectionClientBackend::Couchbase2CollectionBackend(
            Couchbase2CollectionClient::new(),
        ))
    }
}
