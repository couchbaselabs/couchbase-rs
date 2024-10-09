use crate::error;
use couchbase_core::agent::Agent;

pub(crate) struct QueryClient {
    backend: QueryClientBackend,
}

impl QueryClient {
    pub fn new(backend: QueryClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query(&self, query: String) -> error::Result<()> {
        match &self.backend {
            QueryClientBackend::CouchbaseQueryClientBackend(backend) => {
                backend.query(query).await?;
            }
            QueryClientBackend::Couchbase2QueryClientBackend(backend) => {
                backend.query(query).await?;
            }
        }

        Ok(())
    }
}

pub(crate) enum QueryClientBackend {
    CouchbaseQueryClientBackend(CouchbaseQueryClient),
    Couchbase2QueryClientBackend(Couchbase2QueryClient),
}

pub(crate) struct CouchbaseQueryClient {
    agent: Agent,
}

impl CouchbaseQueryClient {
    pub fn new(agent: Agent) -> Self {
        Self { agent }
    }

    async fn query(&self, _query: String) -> error::Result<()> {
        unimplemented!()
    }
}

pub(crate) struct Couchbase2QueryClient {}

impl Couchbase2QueryClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn query(&self, _query: String) -> error::Result<()> {
        unimplemented!()
    }
}
