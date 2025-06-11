use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::collections_mgmt_client::{
    CollectionsMgmtClient, CollectionsMgmtClientBackend, CouchbaseCollectionsMgmtClient,
};
use crate::clients::scope_client::{
    Couchbase2ScopeClient, CouchbaseScopeClient, ScopeClient, ScopeClientBackend,
};
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

    pub fn collections_management_client(&self) -> CollectionsMgmtClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(backend) => {
                let client = backend.collections_management_client();

                CollectionsMgmtClient::new(
                    CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client),
                )
            }
            BucketClientBackend::Couchbase2BucketBackend(_) => {
                unimplemented!()
            }
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
    agent_provider: CouchbaseAgentProvider,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseBucketClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
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
                self.agent_provider.clone(),
                self.name().to_string(),
                name,
                self.default_retry_strategy.clone(),
            ),
        ))
    }

    pub fn collections_management_client(&self) -> CouchbaseCollectionsMgmtClient {
        CouchbaseCollectionsMgmtClient::new(
            self.agent_provider.clone(),
            self.name.clone(),
            self.default_retry_strategy.clone(),
        )
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

    pub fn collections_management_client(&self) -> CouchbaseCollectionsMgmtClient {
        unimplemented!()
    }
}
