use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use futures::{FutureExt, TryFutureExt};

use crate::collectionresolver::{CollectionResolver, orchestrate_memd_collection_id};
use crate::compressionmanager::{CompressionManager, Compressor};
use crate::crudoptions::{GetOptions, UpsertOptions};
use crate::crudresults::{GetResult, UpsertResult};
use crate::error::Result;
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{KvClientManager, KvClientManagerClientType, orchestrate_memd_client};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::request::{GetRequest, SetRequest};
use crate::mutationtoken::MutationToken;
use crate::nmvbhandler::NotMyVbucketConfigHandler;
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager};
use crate::vbucketrouter::{orchestrate_memd_routing, VbucketRouter};

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

    pub(crate) async fn upsert<'a>(&self, opts: UpsertOptions<'a>) -> Result<UpsertResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(false, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, _manifest_id, endpoint, vbucket_id, client| {
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

    pub(crate) async fn get<'a>(&self, opts: GetOptions<'a>) -> Result<GetResult> {
        self.orchestrate_simple_crud(
            opts.key,
            RetryInfo::new(true, opts.retry_strategy),
            opts.scope_name,
            opts.collection_name,
            async |collection_id, _manifest_id, endpoint, vb_id, client| {
                client
                    .get(GetRequest {
                        collection_id,
                        key: opts.key,
                        vbucket_id: vb_id,
                        on_behalf_of: None,
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

    pub(crate) async fn orchestrate_simple_crud<Fut, Resp>(
        &self,
        key: &[u8],
        retry_info: RetryInfo,
        scope_name: &str,
        collection_name: &str,
        operation: impl Fn(u32, u64, String, u16, Arc<KvClientManagerClientType<M>>) -> Fut
            + Send
            + Sync,
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
                                endpoint.clone(),
                                async |client: Arc<KvClientManagerClientType<M>>| {
                                    operation(
                                        collection_id,
                                        manifest_rev,
                                        endpoint.clone(),
                                        vb_id,
                                        client,
                                    )
                                    .await
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
}
