use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::management::buckets::bucket_settings::BucketSettings;
use crate::options::bucket_mgmt_options::{
    CreateBucketOptions, DeleteBucketOptions, FlushBucketOptions, GetAllBucketsOptions,
    GetBucketOptions, UpdateBucketOptions,
};
use couchbase_core::mgmtx;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;

pub(crate) struct BucketMgmtClient {
    backend: BucketMgmtClientBackend,
}

impl BucketMgmtClient {
    pub fn new(backend: BucketMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_all_buckets(
        &self,
        opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.get_all_buckets(opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.get_all_buckets(opts).await
            }
        }
    }

    pub async fn get_bucket(
        &self,
        bucket_name: String,
        opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.get_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.get_bucket(bucket_name, opts).await
            }
        }
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: CreateBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.create_bucket(settings, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.create_bucket(settings, opts).await
            }
        }
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.update_bucket(settings, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.update_bucket(settings, opts).await
            }
        }
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: String,
        opts: DeleteBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.delete_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.delete_bucket(bucket_name, opts).await
            }
        }
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: String,
        opts: FlushBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.flush_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.flush_bucket(bucket_name, opts).await
            }
        }
    }
}

pub(crate) enum BucketMgmtClientBackend {
    CouchbaseBucketMgmtClientBackend(CouchbaseBucketMgmtClient),
    Couchbase2BucketMgmtClientBackend(Couchbase2BucketMgmtClient),
}

pub(crate) struct CouchbaseBucketMgmtClient {
    agent_provider: CouchbaseAgentProvider,

    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseBucketMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            default_retry_strategy,
        }
    }

    pub async fn get_all_buckets(
        &self,
        opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::options::management::GetAllBucketsOptions::new()
            .retry_strategy(self.default_retry_strategy.clone());

        let buckets = agent.get_all_buckets(&opts).await?;

        Ok(buckets.into_iter().map(|b| b.into()).collect())
    }

    pub async fn get_bucket(
        &self,
        bucket_name: String,
        opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::options::management::GetBucketOptions::new(&bucket_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let bucket = agent.get_bucket(&opts).await?;

        Ok(bucket.into())
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: CreateBucketOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;

        let mut core_settings = mgmtx::bucket_settings::BucketSettings::default();
        core_settings = core_settings.ram_quota_mb(settings.ram_quota_mb);

        if let Some(flush_enabled) = settings.flush_enabled {
            core_settings = core_settings.flush_enabled(flush_enabled);
        }

        if let Some(replica_number) = settings.num_replicas {
            core_settings = core_settings.replica_number(replica_number);
        }

        if let Some(eviction_policy) = settings.eviction_policy {
            core_settings = core_settings.eviction_policy(eviction_policy);
        }

        if let Some(max_ttl) = settings.max_expiry {
            core_settings = core_settings.max_ttl(max_ttl);
        }

        if let Some(compression_mode) = settings.compression_mode {
            core_settings = core_settings.compression_mode(compression_mode);
        }

        if let Some(durability_min_level) = settings.minimum_durability_level {
            core_settings = core_settings.durability_min_level(durability_min_level);
        }

        if let Some(history_retention_collection_default) =
            settings.history_retention_collection_default
        {
            core_settings = core_settings
                .history_retention_collection_default(history_retention_collection_default);
        }

        if let Some(history_retention_bytes) = settings.history_retention_bytes {
            core_settings = core_settings.history_retention_bytes(history_retention_bytes);
        }

        if let Some(history_retention_duration) = settings.history_retention_duration {
            core_settings = core_settings
                .history_retention_seconds(history_retention_duration.as_secs() as u32);
        }

        if let Some(conflict_resolution_type) = settings.conflict_resolution_type {
            core_settings = core_settings.conflict_resolution_type(conflict_resolution_type);
        }

        if let Some(replica_index) = settings.replica_indexes {
            core_settings = core_settings.replica_index(replica_index);
        }

        if let Some(bucket_type) = settings.bucket_type {
            core_settings = core_settings.bucket_type(bucket_type);
        }

        if let Some(storage_backend) = settings.storage_backend {
            core_settings = core_settings.storage_backend(storage_backend);
        }

        let opts = couchbase_core::options::management::CreateBucketOptions::new(
            &settings.name,
            &core_settings,
        )
        .retry_strategy(self.default_retry_strategy.clone());

        agent.create_bucket(&opts).await?;

        Ok(())
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;

        let mut core_settings = mgmtx::bucket_settings::BucketSettings::default();
        core_settings = core_settings.ram_quota_mb(settings.ram_quota_mb);

        if let Some(flush_enabled) = settings.flush_enabled {
            core_settings = core_settings.flush_enabled(flush_enabled);
        }

        if let Some(replica_number) = settings.num_replicas {
            core_settings = core_settings.replica_number(replica_number);
        }

        if let Some(eviction_policy) = settings.eviction_policy {
            core_settings = core_settings.eviction_policy(eviction_policy);
        }

        if let Some(max_ttl) = settings.max_expiry {
            core_settings = core_settings.max_ttl(max_ttl);
        }

        if let Some(compression_mode) = settings.compression_mode {
            core_settings = core_settings.compression_mode(compression_mode);
        }

        if let Some(durability_min_level) = settings.minimum_durability_level {
            core_settings = core_settings.durability_min_level(durability_min_level);
        }

        if let Some(history_retention_collection_default) =
            settings.history_retention_collection_default
        {
            core_settings = core_settings
                .history_retention_collection_default(history_retention_collection_default);
        }

        if let Some(history_retention_bytes) = settings.history_retention_bytes {
            core_settings = core_settings.history_retention_bytes(history_retention_bytes);
        }

        if let Some(history_retention_duration) = settings.history_retention_duration {
            core_settings = core_settings
                .history_retention_seconds(history_retention_duration.as_secs() as u32);
        }

        if let Some(conflict_resolution_type) = settings.conflict_resolution_type {
            core_settings = core_settings.conflict_resolution_type(conflict_resolution_type);
        }

        if let Some(replica_indexes) = settings.replica_indexes {
            core_settings = core_settings.replica_index(replica_indexes);
        }

        if let Some(bucket_type) = settings.bucket_type {
            core_settings = core_settings.bucket_type(bucket_type);
        }

        let opts = couchbase_core::options::management::UpdateBucketOptions::new(
            &settings.name,
            &core_settings,
        )
        .retry_strategy(self.default_retry_strategy.clone());

        agent.update_bucket(&opts).await?;

        Ok(())
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: String,
        opts: DeleteBucketOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::options::management::DeleteBucketOptions::new(&bucket_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.delete_bucket(&opts).await?;

        Ok(())
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: String,
        opts: FlushBucketOptions,
    ) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        let opts = couchbase_core::options::management::FlushBucketOptions::new(&bucket_name)
            .retry_strategy(self.default_retry_strategy.clone());
        agent.flush_bucket(&opts).await?;

        Ok(())
    }
}

pub(crate) struct Couchbase2BucketMgmtClient {}

impl Couchbase2BucketMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn get_all_buckets(
        &self,
        _opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        unimplemented!()
    }

    pub async fn get_bucket(
        &self,
        _bucket_name: String,
        _opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        unimplemented!()
    }

    pub async fn create_bucket(
        &self,
        _settings: BucketSettings,
        _opts: CreateBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn update_bucket(
        &self,
        _settings: BucketSettings,
        _opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn delete_bucket(
        &self,
        _bucket_name: String,
        _opts: DeleteBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn flush_bucket(
        &self,
        _bucket_name: String,
        _opts: FlushBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
