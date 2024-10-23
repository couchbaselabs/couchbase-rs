use crate::clients::scope_client::{
    Couchbase2ScopeClient, CouchbaseScopeClient, ScopeClient, ScopeClientBackend,
};
use couchbase_core::agent::Agent;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct BucketClient {
    backend: BucketClientBackend,
}

impl BucketClient {
    pub fn new(bucket_client_backend: BucketClientBackend) -> Self {
        Self {
            backend: bucket_client_backend,
        }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(client) => client.name(),
            BucketClientBackend::Couchbase2BucketBackend(client) => client.name(),
        }
    }

    pub fn scope_client(&self, name: String) -> ScopeClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(client) => client.scope(name),
            BucketClientBackend::Couchbase2BucketBackend(client) => client.scope(name),
        }
    }
}

#[derive(Clone)]
pub(crate) enum BucketClientBackend {
    CouchbaseBucketBackend(CouchbaseBucketClient),
    Couchbase2BucketBackend(Couchbase2BucketClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseBucketClient {
    agent: Agent,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseBucketClient {
    pub fn new(agent: Agent, name: String, default_retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        Self {
            agent,
            name,
            default_retry_strategy,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope(&self, name: String) -> ScopeClient {
        ScopeClient::new(ScopeClientBackend::CouchbaseScopeBackend(
            CouchbaseScopeClient::new(
                self.agent.clone(),
                self.name().to_string(),
                name,
                self.default_retry_strategy.clone(),
            ),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2BucketClient {
    name: String,
}

impl Couchbase2BucketClient {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope(&self, _name: String) -> ScopeClient {
        ScopeClient::new(ScopeClientBackend::Couchbase2ScopeBackend(
            Couchbase2ScopeClient::new(),
        ))
    }
}
