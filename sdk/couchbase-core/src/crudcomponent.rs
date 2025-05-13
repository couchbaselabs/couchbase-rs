use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use crate::collectionresolver::{orchestrate_memd_collection_id, CollectionResolver};
use crate::compressionmanager::{CompressionManager, Compressor};
use crate::crudoptions::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetCollectionIdOptions, GetMetaOptions, GetOptions, IncrementOptions,
    LookupInOptions, MutateInOptions, PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions,
    UpsertOptions,
};
use crate::crudresults::{
    AddResult, AppendResult, DecrementResult, DeleteResult, GetAndLockResult, GetAndTouchResult,
    GetCollectionIdResult, GetMetaResult, GetResult, IncrementResult, LookupInResult,
    MutateInResult, PrependResult, ReplaceResult, SubDocResult, TouchResult, UnlockResult,
    UpsertResult,
};
use crate::error;
use crate::error::{Error, MemdxError, Result};
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{
    orchestrate_memd_client, orchestrate_random_memd_client, KvClientManager,
    KvClientManagerClientType,
};
use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::error::ServerErrorKind;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::request::{
    AddRequest, AppendRequest, DecrementRequest, DeleteRequest, GetAndLockRequest,
    GetAndTouchRequest, GetCollectionIdRequest, GetMetaRequest, GetRequest, IncrementRequest,
    LookupInRequest, MutateInRequest, PrependRequest, ReplaceRequest, SetRequest, TouchRequest,
    UnlockRequest,
};
use crate::memdx::response::{LookupInResponse, MutateInResponse};
use crate::mutationtoken::MutationToken;
use crate::nmvbhandler::NotMyVbucketConfigHandler;
use crate::retry::{error_to_retry_reason, orchestrate_retries, RetryInfo, RetryManager};
use crate::vbucketrouter::{orchestrate_memd_routing, VbucketRouter};
use futures::{FutureExt, TryFutureExt};
use log::debug;
use tokio::time::sleep;

pub(crate) struct CrudComponent<
    M: KvClientManager,
    V: VbucketRouter,
    Nmvb: NotMyVbucketConfigHandler,
    C: CollectionResolver,
    Comp: Compressor,
> {
    conn_manager: Arc<M>,
    router: Arc<V>,
    nmvb_handler: Arc<Nmvb>,
    collections: Arc<C>,
    retry_manager: Arc<RetryManager>,
    compression_manager: Arc<CompressionManager<Comp>>,
}

// TODO: So much clone.
impl<
        M: KvClientManager,
        V: VbucketRouter,
        Nmvb: NotMyVbucketConfigHandler,
        C: CollectionResolver,
        Comp: Compressor,
    > CrudComponent<M, V, Nmvb, C, Comp>
{
    pub(crate) fn new(
        nmvb_handler: Arc<Nmvb>,
        router: Arc<V>,
        conn_manager: Arc<M>,
        collections: Arc<C>,
        retry_manager: Arc<RetryManager>,
        compression_manager: Arc<CompressionManager<Comp>>,
    ) -> Self {
        CrudComponent {
            conn_manager,
            router,
            nmvb_handler,
            collections,
            retry_manager,
            compression_manager,
        }
    }

    pub(crate) async fn upsert(&self, opts: UpsertOptions<'_>) -> Result<UpsertResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, _manifest_id, vbucket_id, client| {
                let mut compressor = self.compression_manager.compressor();
                let (value, datatype) = match compressor.compress(
                    client.has_feature(HelloFeature::Snappy),
                    opts.datatype,
                    opts.value,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(e);
                    }
                };

                client
                    .set(SetRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        flags: opts.flags,
                        value,
                        datatype,
                        expiry: opts.expiry,
                        preserve_expiry: opts.preserve_expiry,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        UpsertResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub(crate) async fn get(&self, opts: GetOptions<'_>) -> Result<GetResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(true, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, _manifest_id, vbucket_id, client| {
                client
                    .get(GetRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| GetResult {
                        value: resp.value,
                        datatype: resp.datatype,
                        cas: resp.cas,
                        flags: resp.flags,
                    })
                    .await
            },
        )
        .await
    }

    pub(crate) async fn get_meta(&self, opts: GetMetaOptions<'_>) -> Result<GetMetaResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(true, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, _manifest_id, vbucket_id, client| {
                client
                    .get_meta(GetMetaRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| GetMetaResult {
                        value: resp.value,
                        datatype: resp.datatype,
                        server_duration: resp.server_duration,
                        expiry: resp.expiry,
                        seq_no: resp.seq_no,
                        cas: resp.cas,
                        flags: resp.flags,
                        deleted: resp.deleted,
                    })
                    .await
            },
        )
        .await
    }

    pub async fn delete(&self, opts: DeleteOptions<'_>) -> Result<DeleteResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .delete(DeleteRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        DeleteResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn get_and_lock(&self, opts: GetAndLockOptions<'_>) -> Result<GetAndLockResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .get_and_lock(GetAndLockRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        lock_time: opts.lock_time,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| GetAndLockResult {
                        value: resp.value,
                        datatype: resp.datatype,
                        cas: resp.cas,
                        flags: resp.flags,
                    })
                    .await
            },
        )
        .await
    }

    pub async fn get_and_touch(&self, opts: GetAndTouchOptions<'_>) -> Result<GetAndTouchResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .get_and_touch(GetAndTouchRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        expiry: opts.expiry,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| GetAndTouchResult {
                        value: resp.value,
                        datatype: resp.datatype,
                        cas: resp.cas,
                        flags: resp.flags,
                    })
                    .await
            },
        )
        .await
    }

    pub async fn unlock(&self, opts: UnlockOptions<'_>) -> Result<UnlockResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .unlock(UnlockRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        cas: opts.cas,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| UnlockResult {
                        // mutation token?
                    })
                    .await
            },
        )
        .await
    }

    pub async fn touch(&self, opts: TouchOptions<'_>) -> Result<TouchResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .touch(TouchRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        expiry: opts.expiry,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| TouchResult { cas: resp.cas })
                    .await
            },
        )
        .await
    }

    pub async fn add(&self, opts: AddOptions<'_>) -> Result<AddResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                let mut compressor = self.compression_manager.compressor();
                let (value, datatype) = match compressor.compress(
                    client.has_feature(HelloFeature::Snappy),
                    opts.datatype,
                    opts.value,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(e);
                    }
                };

                client
                    .add(AddRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        flags: opts.flags,
                        value,
                        datatype,
                        expiry: opts.expiry,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        AddResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn replace(&self, opts: ReplaceOptions<'_>) -> Result<ReplaceResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                let mut compressor = self.compression_manager.compressor();
                let (value, datatype) = match compressor.compress(
                    client.has_feature(HelloFeature::Snappy),
                    opts.datatype,
                    opts.value,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(e);
                    }
                };

                client
                    .replace(ReplaceRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        flags: opts.flags,
                        value,
                        datatype,
                        expiry: opts.expiry,
                        preserve_expiry: opts.preserve_expiry,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        ReplaceResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn append(&self, opts: AppendOptions<'_>) -> Result<AppendResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                let mut compressor = self.compression_manager.compressor();
                let (value, datatype) = match compressor.compress(
                    client.has_feature(HelloFeature::Snappy),
                    DataTypeFlag::None,
                    opts.value,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(e);
                    }
                };

                client
                    .append(AppendRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        value,
                        datatype,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        AppendResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn prepend(&self, opts: PrependOptions<'_>) -> Result<PrependResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                let mut compressor = self.compression_manager.compressor();
                let (value, datatype) = match compressor.compress(
                    client.has_feature(HelloFeature::Snappy),
                    DataTypeFlag::None,
                    opts.value,
                ) {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(e);
                    }
                };

                client
                    .prepend(PrependRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        value,
                        datatype,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        PrependResult {
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn increment(&self, opts: IncrementOptions<'_>) -> Result<IncrementResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .increment(IncrementRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        initial: opts.initial,
                        delta: opts.delta,
                        expiry: opts.expiry,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        IncrementResult {
                            cas: resp.cas,
                            value: resp.value,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn decrement(&self, opts: DecrementOptions<'_>) -> Result<DecrementResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .decrement(DecrementRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        initial: opts.initial,
                        delta: opts.delta,
                        expiry: opts.expiry,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        DecrementResult {
                            cas: resp.cas,
                            value: resp.value,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub async fn lookup_in(&self, opts: LookupInOptions<'_>) -> Result<LookupInResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(true, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .lookup_in(LookupInRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        flags: opts.flags,
                        ops: opts.ops,
                        on_behalf_of: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp: LookupInResponse| LookupInResult {
                        value: resp
                            .ops
                            .into_iter()
                            .map(|o| {
                                let err = o.err.map(|e| {
                                    MemdxError::new(e)
                                        .set_doc_id(opts.key.to_vec())
                                        .set_bucket_name(client.bucket_name().unwrap_or_default())
                                        .set_collection_name(opts.collection_name.to_string())
                                        .set_scope_name(opts.scope_name.to_string())
                                });

                                SubDocResult {
                                    err,
                                    value: o.value,
                                }
                            })
                            .collect(),
                        cas: resp.cas,
                        doc_is_deleted: resp.doc_is_deleted,
                    })
                    .await
            },
        )
        .await
    }

    pub async fn mutate_in(&self, opts: MutateInOptions<'_>) -> Result<MutateInResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, manifest_id, vbucket_id, client| {
                client
                    .mutate_in(MutateInRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id,
                        flags: opts.flags,
                        ops: opts.ops,
                        expiry: opts.expiry,
                        preserve_expiry: opts.preserve_expiry,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_err(|e| {
                        let e = Self::update_memdx_err(
                            client.clone(),
                            e,
                            opts.key.to_vec(),
                            opts.scope_name,
                            opts.collection_name,
                        );

                        Error::new_contextual_memdx_error(e)
                    })
                    .map_ok(|resp: MutateInResponse| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vbucket_id,
                            vbuuid: t.vbuuid,
                            seqno: t.seqno,
                        });

                        MutateInResult {
                            value: resp
                                .ops
                                .into_iter()
                                .map(|o| {
                                    let err = o.err.map(|e| {
                                        MemdxError::new(e)
                                            .set_doc_id(opts.key.to_vec())
                                            .set_bucket_name(
                                                client.bucket_name().unwrap_or_default(),
                                            )
                                            .set_collection_name(opts.collection_name.to_string())
                                            .set_scope_name(opts.scope_name.to_string())
                                    });

                                    SubDocResult {
                                        err,
                                        value: o.value,
                                    }
                                })
                                .collect(),
                            cas: resp.cas,
                            mutation_token,
                        }
                    })
                    .await
            },
        )
        .await
    }

    pub(crate) async fn get_collection_id(
        &self,
        opts: GetCollectionIdOptions<'_>,
    ) -> Result<GetCollectionIdResult> {
        let mut retry_info = RetryInfo::new(true, opts.retry_strategy);

        loop {
            let err =
                match orchestrate_random_memd_client(self.conn_manager.clone(), async |client| {
                    client
                        .get_collection_id(GetCollectionIdRequest {
                            scope_name: opts.scope_name,
                            collection_name: opts.collection_name,
                        })
                        .map_err(|e| {
                            let e = e
                                .set_bucket_name(client.bucket_name().unwrap_or_default())
                                .set_collection_name(opts.collection_name.to_string())
                                .set_scope_name(opts.scope_name.to_string());

                            Error::new_contextual_memdx_error(e)
                        })
                        .map_ok(|resp| GetCollectionIdResult {
                            collection_id: resp.collection_id,
                            manifest_rev: resp.manifest_rev,
                        })
                        .await
                })
                .await
                {
                    Ok(r) => {
                        return Ok(r);
                    }
                    Err(e) => e,
                };

            if let Some(memdx_err) = err.is_memdx_error() {
                if memdx_err.is_server_error_kind(ServerErrorKind::UnknownCollectionName) {
                    return Err(err);
                }
            }

            if let Some(reason) = error_to_retry_reason(&self.retry_manager, &err) {
                if let Some(duration) = self
                    .retry_manager
                    .maybe_retry(&mut retry_info, reason)
                    .await
                {
                    debug!(
                        "Retrying operation after {:?} due to {:?}",
                        duration, reason
                    );
                    sleep(duration).await;
                    continue;
                }
            }

            return Err(err);
        }
    }

    pub(crate) async fn orchestrate_simple_crud<Fut, Resp>(
        &self,
        key: &[u8],
        retry_info: RetryInfo,
        scope_name: &str,
        collection_name: &str,
        operation: impl Fn(u32, u64, u16, Arc<KvClientManagerClientType<M>>) -> Fut + Send + Sync,
    ) -> Result<Resp>
    where
        Fut: Future<Output = Result<Resp>> + Send,
        Resp: Send,
    {
        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            orchestrate_memd_collection_id(
                self.collections.clone(),
                scope_name,
                collection_name,
                async |collection_id: u32, manifest_rev: u64| {
                    orchestrate_memd_routing(
                        self.router.clone(),
                        self.nmvb_handler.clone(),
                        key,
                        0,
                        async |endpoint: String, vb_id: u16| {
                            orchestrate_memd_client(
                                self.conn_manager.clone(),
                                &endpoint,
                                async |client: Arc<KvClientManagerClientType<M>>| {
                                    operation(collection_id, manifest_rev, vb_id, client).await
                                },
                            )
                            .await
                        },
                    )
                    .await
                },
            )
            .await
        })
        .await
    }

    fn update_memdx_err(
        client: Arc<KvClientManagerClientType<M>>,
        e: MemdxError,
        key: Vec<u8>,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> MemdxError {
        e.set_doc_id(key)
            .set_bucket_name(client.bucket_name().unwrap_or_default())
            .set_collection_name(collection_name.into())
            .set_scope_name(scope_name.into())
    }
}
