use crate::error;
use crate::mutation_state::MutationToken;
use crate::options::kv_binary_options::{
    AppendOptions, DecrementOptions, IncrementOptions, PrependOptions,
};
use crate::options::kv_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::{ExistsResult, GetResult, MutationResult, TouchResult};
use bytes::Bytes;
use couchbase_core::agent::Agent;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct CouchbaseCoreKvClient {
    agent: Agent,
    bucket_name: String,
    scope_name: String,
    collection_name: String,

    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseCoreKvClient {
    pub fn new(
        agent: Agent,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent,
            bucket_name,
            scope_name,
            collection_name,
            default_retry_strategy,
        }
    }

    pub async fn upsert(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: UpsertOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .upsert(
                couchbase_core::crudoptions::UpsertOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .flags(flags)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .expiry(options.expiry.map(|e| e.as_millis() as u32))
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .preserve_expiry(options.preserve_expiry)
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn insert(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: InsertOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .add(
                couchbase_core::crudoptions::AddOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .flags(flags)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .expiry(options.expiry.map(|e| e.as_millis() as u32))
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn replace(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .replace(
                couchbase_core::crudoptions::ReplaceOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .flags(flags)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .expiry(options.expiry.map(|e| e.as_millis() as u32))
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .preserve_expiry(options.preserve_expiry)
                    .cas(options.cas)
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn remove(
        &self,
        id: String,
        options: RemoveOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .delete(
                couchbase_core::crudoptions::DeleteOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .cas(options.cas)
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn get(&self, id: String, options: GetOptions) -> error::Result<GetResult> {
        let res = self
            .agent
            .get(
                couchbase_core::crudoptions::GetOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .build(),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn exists(&self, id: String, options: ExistsOptions) -> error::Result<ExistsResult> {
        let res = self
            .agent
            .get_meta(
                couchbase_core::crudoptions::GetMetaOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .build(),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_touch(
        &self,
        id: String,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        let res = self
            .agent
            .get_and_touch(
                couchbase_core::crudoptions::GetAndTouchOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .expiry(expiry.as_secs() as u32)
                    .build(),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_lock(
        &self,
        id: String,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        let res = self
            .agent
            .get_and_lock(
                couchbase_core::crudoptions::GetAndLockOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .lock_time(lock_time.as_secs() as u32)
                    .build(),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn unlock(&self, id: String, cas: u64, options: UnlockOptions) -> error::Result<()> {
        self.agent
            .unlock(
                couchbase_core::crudoptions::UnlockOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .cas(cas)
                    .build(),
            )
            .await?;

        Ok(())
    }

    pub async fn touch(
        &self,
        id: String,
        expiry: Duration,
        options: TouchOptions,
    ) -> error::Result<TouchResult> {
        let result = self
            .agent
            .touch(
                couchbase_core::crudoptions::TouchOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .expiry(expiry.as_secs() as u32)
                    .build(),
            )
            .await?;

        Ok(result.into())
    }

    pub async fn append(
        &self,
        id: String,
        value: Vec<u8>,
        options: AppendOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .append(
                couchbase_core::crudoptions::AppendOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn prepend(
        &self,
        id: String,
        value: Vec<u8>,
        options: PrependOptions,
    ) -> error::Result<MutationResult> {
        let result = self
            .agent
            .prepend(
                couchbase_core::crudoptions::PrependOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .build(),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn increment(
        &self,
        id: String,
        options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        let result = self
            .agent
            .increment(
                couchbase_core::crudoptions::IncrementOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .expiry(options.expiry.map(|e| e.as_secs() as u32))
                    .initial(options.initial)
                    .delta(options.delta)
                    .build(),
            )
            .await?;

        Ok(CounterResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            content: result.value,
        })
    }

    pub async fn decrement(
        &self,
        id: String,
        options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        let result = self
            .agent
            .decrement(
                couchbase_core::crudoptions::DecrementOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .durability_level(options.durability_level.map(|l| l.into()))
                    .retry_strategy(
                        options
                            .retry_strategy
                            .unwrap_or(self.default_retry_strategy.clone()),
                    )
                    .expiry(options.expiry.map(|e| e.as_secs() as u32))
                    .initial(options.initial)
                    .delta(options.delta)
                    .build(),
            )
            .await?;

        Ok(CounterResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            content: result.value,
        })
    }
}
