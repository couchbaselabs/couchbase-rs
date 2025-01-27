use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllIndexesOptions,
    WatchQueryIndexOptions,
};
use crate::results::query_index_mgmt_results::QueryIndex;

pub(crate) struct QueryIndexMgmtClient {
    backend: QueryIndexMgmtClientBackend,
}

impl QueryIndexMgmtClient {
    pub fn new(backend: QueryIndexMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_all_indexes(
        &self,
        opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
        }
    }

    pub async fn create_index(
        &self,
        index_name: String,
        fields: Vec<String>,
        opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.create_index(index_name, fields, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.create_index(index_name, fields, opts).await
            }
        }
    }

    pub async fn create_primary_index(
        &self,
        opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.create_primary_index(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.create_primary_index(opts).await
            }
        }
    }

    pub async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
        }
    }

    pub async fn drop_primary_index(
        &self,
        opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.drop_primary_index(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.drop_primary_index(opts).await
            }
        }
    }

    pub async fn watch_indexes(
        &self,
        index_names: Vec<String>,
        opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.watch_indexes(index_names, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.watch_indexes(index_names, opts).await
            }
        }
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.build_deferred_indexes(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.build_deferred_indexes(opts).await
            }
        }
    }
}

pub(crate) enum QueryIndexMgmtClientBackend {
    CouchbaseQueryIndexMgmtClientBackend(CouchbaseQueryIndexMgmtClient),
    Couchbase2QueryIndexMgmtClientBackend(Couchbase2QueryIndexMgmtClient),
}

pub(crate) struct QueryIndexKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
    pub collection_name: String,
}

pub(crate) struct CouchbaseQueryIndexMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: QueryIndexKeyspace,
}

impl CouchbaseQueryIndexMgmtClient {
    pub fn new(agent_provider: CouchbaseAgentProvider, keyspace: QueryIndexKeyspace) -> Self {
        Self {
            agent_provider,
            keyspace,
        }
    }

    async fn get_all_indexes(
        &self,
        opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        let opts = opts.unwrap_or_default();

        let mut get_indexes_opts =
            couchbase_core::queryoptions::GetAllIndexesOptions::new(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .collection_name(&self.keyspace.collection_name);

        if let Some(retry_strategy) = opts.retry_strategy {
            get_indexes_opts = get_indexes_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        let indexes = agent.get_all_indexes(&get_indexes_opts).await?;

        Ok(indexes.into_iter().map(QueryIndex::from).collect())
    }

    async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: Vec<String>,
        opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let index_name = index_name.into();

        let fields: Vec<&str> = fields.iter().map(String::as_str).collect();

        let mut create_index_opts = couchbase_core::queryoptions::CreateIndexOptions::new(
            &self.keyspace.bucket_name,
            &index_name,
            &fields,
        )
        .scope_name(&self.keyspace.scope_name)
        .collection_name(&self.keyspace.collection_name);

        if let Some(deferred) = opts.deferred {
            create_index_opts = create_index_opts.deferred(deferred);
        }
        if let Some(ignore_if_exists) = opts.ignore_if_exists {
            create_index_opts = create_index_opts.ignore_if_exists(ignore_if_exists);
        }
        if let Some(num_replicas) = opts.num_replicas {
            create_index_opts = create_index_opts.num_replicas(num_replicas);
        }
        if let Some(retry_strategy) = opts.retry_strategy {
            create_index_opts = create_index_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.create_index(&create_index_opts).await?;
        Ok(())
    }

    async fn create_primary_index(
        &self,
        opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let mut create_index_opts = couchbase_core::queryoptions::CreatePrimaryIndexOptions::new(
            &self.keyspace.bucket_name,
        )
        .scope_name(&self.keyspace.scope_name)
        .collection_name(&self.keyspace.collection_name);

        if let Some(deferred) = opts.deferred {
            create_index_opts = create_index_opts.deferred(deferred);
        }
        if let Some(ignore_if_exists) = opts.ignore_if_exists {
            create_index_opts = create_index_opts.ignore_if_exists(ignore_if_exists);
        }
        if let Some(num_replicas) = opts.num_replicas {
            create_index_opts = create_index_opts.num_replicas(num_replicas);
        }
        if let Some(index_name) = opts.index_name.as_deref() {
            create_index_opts = create_index_opts.index_name(index_name);
        }
        if let Some(retry_strategy) = opts.retry_strategy {
            create_index_opts = create_index_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.create_primary_index(&create_index_opts).await?;

        Ok(())
    }

    async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let mut drop_index_opts = couchbase_core::queryoptions::DropIndexOptions::new(
            &self.keyspace.bucket_name,
            index_name.as_str(),
        )
        .scope_name(&self.keyspace.scope_name)
        .collection_name(&self.keyspace.collection_name);

        if let Some(ignore) = opts.ignore_if_not_exists {
            drop_index_opts = drop_index_opts.ignore_if_not_exists(ignore);
        }
        if let Some(retry_strategy) = opts.retry_strategy {
            drop_index_opts = drop_index_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.drop_index(&drop_index_opts).await?;

        Ok(())
    }

    async fn drop_primary_index(
        &self,
        opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let mut drop_index_opts =
            couchbase_core::queryoptions::DropPrimaryIndexOptions::new(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .collection_name(&self.keyspace.collection_name);

        if let Some(ignore) = opts.ignore_if_not_exists {
            drop_index_opts = drop_index_opts.ignore_if_not_exists(ignore);
        }
        if let Some(index_name) = opts.index_name.as_deref() {
            drop_index_opts = drop_index_opts.index_name(index_name);
        }
        if let Some(retry_strategy) = opts.retry_strategy {
            drop_index_opts = drop_index_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.drop_primary_index(&drop_index_opts).await?;

        Ok(())
    }

    async fn watch_indexes(
        &self,
        index_names: Vec<String>,
        opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let index_names_refs: Vec<&str> = index_names.iter().map(String::as_str).collect();

        let mut watch_indexes_opts = couchbase_core::queryoptions::WatchIndexesOptions::new(
            &self.keyspace.bucket_name,
            &index_names_refs,
        )
        .scope_name(&self.keyspace.scope_name)
        .collection_name(&self.keyspace.collection_name);

        if let Some(watch_primary) = opts.watch_primary {
            watch_indexes_opts = watch_indexes_opts.watch_primary(watch_primary);
        }
        if let Some(retry_strategy) = opts.retry_strategy {
            watch_indexes_opts = watch_indexes_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.watch_indexes(&watch_indexes_opts).await?;

        Ok(())
    }

    async fn build_deferred_indexes(
        &self,
        opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();

        let mut build_indexes_opts =
            couchbase_core::queryoptions::BuildDeferredIndexesOptions::new(
                &self.keyspace.bucket_name,
            )
            .scope_name(&self.keyspace.scope_name)
            .collection_name(&self.keyspace.collection_name);

        if let Some(retry_strategy) = opts.retry_strategy {
            build_indexes_opts = build_indexes_opts.retry_strategy(retry_strategy);
        }

        let agent = self.agent_provider.get_agent().await;

        agent.build_deferred_indexes(&build_indexes_opts).await?;

        Ok(())
    }
}

pub(crate) struct Couchbase2QueryIndexMgmtClient {}

impl Couchbase2QueryIndexMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn get_all_indexes(
        &self,
        _opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        unimplemented!()
    }

    async fn create_index(
        &self,
        _index_name: String,
        _fields: Vec<String>,
        _opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn create_primary_index(
        &self,
        _opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn drop_index(
        &self,
        _index_name: String,
        _opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn drop_primary_index(
        &self,
        _opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn watch_indexes(
        &self,
        _index_names: Vec<String>,
        _opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn build_deferred_indexes(
        &self,
        _opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
