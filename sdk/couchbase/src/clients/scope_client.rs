use crate::clients::collection_client::{
    CollectionClient, CollectionClientBackend, Couchbase2CollectionClient,
    CouchbaseCollectionClient,
};
use crate::clients::query_client::{CouchbaseQueryClient, QueryClient, QueryClientBackend};
use couchbase_core::agent::Agent;

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
}

impl CouchbaseScopeClient {
    pub fn new(agent: Agent, bucket_name: String, name: String) -> Self {
        Self {
            agent,
            bucket_name,
            name,
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
            ),
        ))
    }

    pub fn query_client(&self) -> CouchbaseQueryClient {
        CouchbaseQueryClient::new(self.agent.clone())
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
