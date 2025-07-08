use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::management::collections::collection_settings::{
    CreateCollectionSettings, MaxExpiryValue, UpdateCollectionSettings,
};
use crate::options::collection_mgmt_options::*;
use crate::results::collections_mgmt_results::{CollectionSpec, ScopeSpec};
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

pub(crate) struct CollectionsMgmtClient {
    backend: CollectionsMgmtClientBackend,
}

impl CollectionsMgmtClient {
    pub fn new(backend: CollectionsMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: CreateScopeOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.create_scope(scope_name, opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.create_scope(scope_name, opts).await
            }
        }
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: DropScopeOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.drop_scope(scope_name, opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.drop_scope(scope_name, opts).await
            }
        }
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .create_collection(scope_name, collection_name, settings, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .create_collection(scope_name, collection_name, settings, opts)
                    .await
            }
        }
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .update_collection(scope_name, collection_name, settings, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .update_collection(scope_name, collection_name, settings, opts)
                    .await
            }
        }
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: DropCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .drop_collection(scope_name, collection_name, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .drop_collection(scope_name, collection_name, opts)
                    .await
            }
        }
    }

    pub async fn get_all_scopes(&self, opts: GetAllScopesOptions) -> error::Result<Vec<ScopeSpec>> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.get_all_scopes(opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.get_all_scopes(opts).await
            }
        }
    }
}

pub(crate) enum CollectionsMgmtClientBackend {
    CouchbaseCollectionsMgmtClientBackend(CouchbaseCollectionsMgmtClient),
    Couchbase2CollectionsMgmtClientBackend(Couchbase2CollectionsMgmtClient),
}

pub(crate) struct CouchbaseCollectionsMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: String,

    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseCollectionsMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name,
            default_retry_strategy,
        }
    }

    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: CreateScopeOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        agent
            .create_scope(
                &couchbase_core::options::management::CreateScopeOptions::new(
                    &self.bucket_name,
                    scope_name.into().as_str(),
                )
                .retry_strategy(self.default_retry_strategy.clone()),
            )
            .await?;

        Ok(())
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: DropScopeOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        agent
            .delete_scope(
                &couchbase_core::options::management::DeleteScopeOptions::new(
                    &self.bucket_name,
                    scope_name.into().as_str(),
                )
                .retry_strategy(self.default_retry_strategy.clone()),
            )
            .await?;

        Ok(())
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let scope_name = scope_name.into();
        let collection_name = collection_name.into();
        let mut opts = couchbase_core::options::management::CreateCollectionOptions::new(
            &self.bucket_name,
            scope_name.as_str(),
            collection_name.as_str(),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        if let Some(max_ttl) = settings.max_expiry {
            opts = opts.max_ttl(max_ttl.into());
        }

        if let Some(history_enabled) = settings.history {
            opts = opts.history_enabled(history_enabled);
        }

        agent.create_collection(&opts).await?;

        Ok(())
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let scope_name = scope_name.into();
        let collection_name = collection_name.into();
        let mut opts = couchbase_core::options::management::UpdateCollectionOptions::new(
            &self.bucket_name,
            scope_name.as_str(),
            collection_name.as_str(),
        )
        .retry_strategy(self.default_retry_strategy.clone());

        if let Some(max_ttl) = settings.max_expiry {
            opts = opts.max_ttl(max_ttl.into());
        }

        if let Some(history_enabled) = settings.history {
            opts = opts.history_enabled(history_enabled);
        }

        agent.update_collection(&opts).await?;

        Ok(())
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: DropCollectionOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        agent
            .delete_collection(
                &couchbase_core::options::management::DeleteCollectionOptions::new(
                    &self.bucket_name,
                    scope_name.into().as_str(),
                    collection_name.into().as_str(),
                )
                .retry_strategy(self.default_retry_strategy.clone()),
            )
            .await?;

        Ok(())
    }

    pub async fn get_all_scopes(&self, opts: GetAllScopesOptions) -> error::Result<Vec<ScopeSpec>> {
        let agent = self.agent_provider.get_agent().await;
        let manifest = agent
            .get_collection_manifest(
                &couchbase_core::options::management::GetCollectionManifestOptions::new(
                    &self.bucket_name,
                )
                .retry_strategy(self.default_retry_strategy.clone()),
            )
            .await?;

        let mut scopes = vec![];
        for scope in manifest.scopes {
            let mut collections = vec![];
            for collection in scope.collections {
                collections.push(CollectionSpec {
                    name: collection.name,
                    scope_name: scope.name.clone(),
                    max_expiry: collection
                        .max_ttl
                        .map(MaxExpiryValue::from)
                        .unwrap_or(MaxExpiryValue::InheritFromBucket),
                    history: collection.history.unwrap_or(false),
                })
            }

            scopes.push(ScopeSpec {
                name: scope.name,
                collections,
            });
        }

        Ok(scopes)
    }
}

pub(crate) struct Couchbase2CollectionsMgmtClient {}

impl Couchbase2CollectionsMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn create_scope(
        &self,
        _scope_name: impl Into<String>,
        _opts: CreateScopeOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_scope(
        &self,
        _scope_name: impl Into<String>,
        _opts: DropScopeOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn create_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _settings: CreateCollectionSettings,
        _opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn update_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _settings: UpdateCollectionSettings,
        _opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _opts: DropCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn get_all_scopes(
        &self,
        _opts: GetAllScopesOptions,
    ) -> error::Result<Vec<ScopeSpec>> {
        unimplemented!()
    }
}
