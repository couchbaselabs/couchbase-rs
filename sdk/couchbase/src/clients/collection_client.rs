use crate::clients::core_kv_client::{CoreKvClient, CoreKvClientBackend, Couchbase2CoreKvClient};
use crate::clients::couchbase_core_kv_client::CouchbaseCoreKvClient;
use couchbase_core::agent::Agent;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct CollectionClient {
    backend: CollectionClientBackend,
}

impl CollectionClient {
    pub fn new(backend: CollectionClientBackend) -> Self {
        Self { backend }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => client.name.as_str(),
            CollectionClientBackend::Couchbase2CollectionBackend(client) => client.name.as_str(),
        }
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => client.core_kv_client(),
            CollectionClientBackend::Couchbase2CollectionBackend(client) => client.core_kv_client(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum CollectionClientBackend {
    CouchbaseCollectionBackend(CouchbaseCollectionClient),
    Couchbase2CollectionBackend(Couchbase2CollectionClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseCollectionClient {
    agent: Agent,
    bucket_name: String,
    scope_name: String,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseCollectionClient {
    pub fn new(
        agent: Agent,
        bucket_name: String,
        scope_name: String,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent,
            bucket_name,
            scope_name,
            name,
            default_retry_strategy,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        CoreKvClient::new(CoreKvClientBackend::CouchbaseCoreKvClientBackend(
            CouchbaseCoreKvClient::new(
                self.agent.clone(),
                self.bucket_name().to_string(),
                self.scope_name().to_string(),
                self.name().to_string(),
                self.default_retry_strategy.clone(),
            ),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2CollectionClient {
    // bucket_name: String,
    // scope_name: String,
    name: String,
}

impl Couchbase2CollectionClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        CoreKvClient::new(CoreKvClientBackend::Couchbase2CoreKvClientBackend(
            Couchbase2CoreKvClient::new(),
        ))
    }
}
