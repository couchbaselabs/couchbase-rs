use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, TryFutureExt};
use tokio::time::sleep;

use crate::collectionresolver::{CollectionResolver, orchestrate_memd_collection_id};
use crate::crudoptions::{GetOptions, UpsertOptions};
use crate::crudresults::{GetResult, UpsertResult};
use crate::error::{ErrorKind, Result};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{KvClientManager, KvClientManagerClientType, orchestrate_memd_client};
use crate::memdx::request::{GetRequest, SetRequest};
use crate::mutationtoken::MutationToken;
use crate::nmvbhandler::NotMyVbucketConfigHandler;
use crate::vbucketrouter::{orchestrate_memd_routing, VbucketRouter};

pub(crate) struct CrudComponent<
    M: KvClientManager,
    V: VbucketRouter,
    Nmvb: NotMyVbucketConfigHandler,
    C: CollectionResolver,
> {
    conn_manager: Arc<M>,
    router: Arc<V>,
    nmvb_handler: Arc<Nmvb>,
    collections: Arc<C>,
}

// TODO: So much clone.
impl<
        M: KvClientManager,
        V: VbucketRouter,
        Nmvb: NotMyVbucketConfigHandler,
        C: CollectionResolver,
    > CrudComponent<M, V, Nmvb, C>
{
    pub(crate) fn new(
        nmvb_handler: Arc<Nmvb>,
        router: Arc<V>,
        conn_manager: Arc<M>,
        collections: Arc<C>,
    ) -> Self {
        CrudComponent {
            conn_manager,
            router,
            nmvb_handler,
            collections,
        }
    }

    pub(crate) async fn upsert(&self, opts: UpsertOptions) -> Result<UpsertResult> {
        self.orchestrate_simple_crud(
            &opts.key,
            &opts.scope_name,
            &opts.collection_name,
            async |collection_id, _manifest_id, endpoint, vb_id, client| {
                client
                    .set(SetRequest {
                        collection_id,
                        key: opts.key.clone(),
                        vbucket_id: vb_id,
                        flags: opts.flags,
                        value: opts.value.clone(),
                        datatype: 0,
                        expiry: opts.expiry,
                        preserve_expiry: opts.preserve_expiry,
                        cas: opts.cas,
                        on_behalf_of: None,
                        durability_level: opts.durability_level,
                        durability_level_timeout: None,
                    })
                    .map_ok(|resp| {
                        let mutation_token = resp.mutation_token.map(|t| MutationToken {
                            vbid: vb_id,
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

    pub(crate) async fn get(&self, opts: GetOptions) -> Result<GetResult> {
        self.orchestrate_simple_crud(
            &opts.key,
            &opts.scope_name,
            &opts.collection_name,
            async |collection_id, _manifest_id, endpoint, vb_id, client| {
                client
                    .get(GetRequest {
                        collection_id,
                        key: opts.key.clone(),
                        vbucket_id: vb_id,
                        on_behalf_of: None,
                    })
                    .map_ok(|resp| resp.into())
                    .await
            },
        )
        .await
    }

    pub(crate) async fn orchestrate_simple_crud<Fut, Resp>(
        &self,
        key: &[u8],
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
        loop {
            match orchestrate_memd_collection_id(
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
            {
                Ok(res) => {
                    return Ok(res);
                }
                Err(e) => match e.kind.as_ref() {
                    ErrorKind::InvalidVbucketMap => {
                        // TODO: this can only be temporary.
                        sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    _ => {
                        return Err(e);
                    }
                },
            }
        }
    }
}
