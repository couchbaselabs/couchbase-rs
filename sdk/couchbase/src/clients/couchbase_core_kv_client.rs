use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::mutation_state::MutationToken;
use crate::options::kv_binary_options::{
    AppendOptions, DecrementOptions, IncrementOptions, PrependOptions,
};
use crate::options::kv_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::{
    ExistsResult, GetResult, LookupInResult, LookupInResultEntry, MutateInResult,
    MutateInResultEntry, MutationResult, TouchResult,
};
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use couchbase_core::memdx::subdoc::{reorder_subdoc_ops, SubdocDocFlag};
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct CouchbaseCoreKvClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: String,
    scope_name: String,
    collection_name: String,

    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseCoreKvClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name,
            scope_name,
            collection_name,
            default_retry_strategy,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    pub async fn upsert(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: UpsertOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .upsert(
                couchbase_core::crudoptions::UpsertOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(|e| e.as_millis() as u32))
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                )
                .preserve_expiry(options.preserve_expiry),
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
        id: &str,
        value: &[u8],
        flags: u32,
        options: InsertOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .add(
                couchbase_core::crudoptions::AddOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(|e| e.as_millis() as u32))
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
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
        id: &str,
        value: &[u8],
        flags: u32,
        options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .replace(
                couchbase_core::crudoptions::ReplaceOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .flags(flags)
                .expiry(options.expiry.map(|e| e.as_millis() as u32))
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                )
                .preserve_expiry(options.preserve_expiry)
                .cas(options.cas),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn remove(&self, id: &str, options: RemoveOptions) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .delete(
                couchbase_core::crudoptions::DeleteOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                )
                .cas(options.cas),
            )
            .await?;

        Ok(MutationResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
        })
    }

    pub async fn get(&self, id: &str, options: GetOptions) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let res = agent
            .get(
                couchbase_core::crudoptions::GetOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn exists(&self, id: &str, options: ExistsOptions) -> error::Result<ExistsResult> {
        let agent = self.agent_provider.get_agent().await;
        let res = agent
            .get_meta(
                couchbase_core::crudoptions::GetMetaOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_touch(
        &self,
        id: &str,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let res = agent
            .get_and_touch(
                couchbase_core::crudoptions::GetAndTouchOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    expiry.as_secs() as u32,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn get_and_lock(
        &self,
        id: &str,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        let agent = self.agent_provider.get_agent().await;
        let res = agent
            .get_and_lock(
                couchbase_core::crudoptions::GetAndLockOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    lock_time.as_secs() as u32,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(res.into())
    }

    pub async fn unlock(&self, id: &str, cas: u64, options: UnlockOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;
        agent
            .unlock(
                couchbase_core::crudoptions::UnlockOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    cas,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn touch(
        &self,
        id: &str,
        expiry: Duration,
        options: TouchOptions,
    ) -> error::Result<TouchResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .touch(
                couchbase_core::crudoptions::TouchOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    expiry.as_secs() as u32,
                )
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        Ok(result.into())
    }

    pub async fn append(
        &self,
        id: &str,
        value: &[u8],
        options: AppendOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .append(
                couchbase_core::crudoptions::AppendOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
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
        id: &str,
        value: &[u8],
        options: PrependOptions,
    ) -> error::Result<MutationResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .prepend(
                couchbase_core::crudoptions::PrependOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    value,
                )
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
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
        id: &str,
        options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .increment(
                couchbase_core::crudoptions::IncrementOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                )
                .expiry(options.expiry.map(|e| e.as_secs() as u32))
                .initial(options.initial)
                .delta(options.delta),
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
        id: &str,
        options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        let agent = self.agent_provider.get_agent().await;
        let result = agent
            .decrement(
                couchbase_core::crudoptions::DecrementOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                )
                .durability_level(options.durability_level.map(|l| l.into()))
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                )
                .expiry(options.expiry.map(|e| e.as_secs() as u32))
                .initial(options.initial)
                .delta(options.delta),
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

    pub async fn lookup_in(
        &self,
        id: &str,
        specs: &[LookupInSpec],
        options: LookupInOptions,
    ) -> error::Result<LookupInResult> {
        let agent = self.agent_provider.get_agent().await;
        let (ordered_specs, op_indexes) = reorder_subdoc_ops(specs);

        let result = agent
            .lookup_in(
                couchbase_core::crudoptions::LookupInOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    ordered_specs
                        .iter()
                        .map(|spec| (*spec).into())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .flags({
                    let mut flags = SubdocDocFlag::empty();
                    if options.access_deleted.unwrap_or(false) {
                        flags |= SubdocDocFlag::AccessDeleted;
                    }
                    flags
                })
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        let mut entries = vec![None; specs.len()];

        for (i, x) in result.value.into_iter().enumerate() {
            let original_idx = op_indexes[i];
            entries[original_idx] = Some(LookupInResultEntry {
                value: x.value.as_ref().map(|v| bytes::Bytes::from(v.clone())),
                error: x.err.as_ref().map(|e| e.clone().into()),
                op: ordered_specs[i].op.clone(),
            });
        }

        Ok(LookupInResult {
            cas: result.cas,
            entries: entries.into_iter().map(|x| x.unwrap()).collect(),
            is_deleted: result.doc_is_deleted,
        })
    }

    pub async fn mutate_in(
        &self,
        id: &str,
        specs: &[MutateInSpec],
        options: MutateInOptions,
    ) -> error::Result<MutateInResult> {
        let agent = self.agent_provider.get_agent().await;
        let (ordered_specs, op_indexes) = reorder_subdoc_ops(specs);

        let result = agent
            .mutate_in(
                couchbase_core::crudoptions::MutateInOptions::new(
                    id.as_bytes(),
                    &self.scope_name,
                    &self.collection_name,
                    ordered_specs
                        .iter()
                        .map(|spec| (*spec).into())
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .flags({
                    let mut flags = SubdocDocFlag::empty();
                    if options.access_deleted.unwrap_or(false) {
                        flags |= SubdocDocFlag::AccessDeleted;
                    }
                    match options.store_semantics {
                        Some(StoreSemantics::Insert) => flags |= SubdocDocFlag::AddDoc,
                        Some(StoreSemantics::Upsert) => flags |= SubdocDocFlag::MkDoc,
                        _ => {}
                    }
                    flags
                })
                .preserve_expiry(options.preserve_expiry)
                .expiry(options.expiry.map(|e| e.as_secs() as u32))
                .cas(options.cas)
                .retry_strategy(
                    options
                        .retry_strategy
                        .unwrap_or(self.default_retry_strategy.clone()),
                ),
            )
            .await?;

        let mut entries = vec![None; specs.len()];

        for (i, x) in result.value.into_iter().enumerate() {
            let original_idx = op_indexes[i];
            entries[original_idx] = Some(MutateInResultEntry {
                value: x.value.as_ref().map(|v| bytes::Bytes::from(v.clone())),
            });
        }

        Ok(MutateInResult {
            cas: result.cas,
            mutation_token: result
                .mutation_token
                .map(|t| MutationToken::new(t, self.bucket_name.clone())),
            entries: entries.into_iter().map(|x| x.unwrap()).collect(),
        })
    }
}
