#![feature(async_closure)]

use std::future::Future;
use std::sync::Arc;

use futures::{FutureExt, TryFutureExt};

use crate::crudoptions::{GetOptions, UpsertOptions};
use crate::crudresults::{GetResult, UpsertResult};
use crate::error::Result;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{KvClientManager, KvClientManagerClientType, orchestrate_memd_client};
use crate::memdx::request::{GetRequest, SetRequest};
use crate::memdx::response::TryFromClientResponse;
use crate::mutationtoken::MutationToken;
use crate::vbucketrouter::{NotMyVbucketConfigHandler, orchestrate_memd_routing, VbucketRouter};

pub(crate) struct CrudComponent<
    M: KvClientManager,
    V: VbucketRouter,
    Nmvb: NotMyVbucketConfigHandler,
> {
    conn_manager: Arc<M>,
    router: Arc<V>,
    nmvb_handler: Arc<Nmvb>,
}

pub(crate) async fn orchestrate_simple_crud<M, V, Fut, Resp>(
    nmvb_handler: Arc<impl NotMyVbucketConfigHandler>,
    vb: Arc<V>,
    mgr: Arc<M>,
    key: &[u8],
    operation: impl Fn(String, u16, Arc<KvClientManagerClientType<M>>) -> Fut + Send + Sync,
) -> Result<Resp>
where
    M: KvClientManager,
    V: VbucketRouter,
    Fut: Future<Output = Result<Resp>> + Send,
    Resp: Send,
{
    orchestrate_memd_routing(
        vb.clone(),
        nmvb_handler,
        key,
        0,
        async |endpoint: String, vb_id: u16| {
            orchestrate_memd_client(
                mgr.clone(),
                endpoint.clone(),
                async |client: Arc<KvClientManagerClientType<M>>| {
                    operation(endpoint.clone(), vb_id, client).await
                },
            )
            .await
        },
    )
    .await
}

// TODO: So much clone.
impl<M: KvClientManager, V: VbucketRouter, Nmvb: NotMyVbucketConfigHandler>
    CrudComponent<M, V, Nmvb>
{
    pub(crate) fn new(nmvb_handler: Arc<Nmvb>, router: Arc<V>, conn_manager: Arc<M>) -> Self {
        CrudComponent {
            conn_manager,
            router,
            nmvb_handler,
        }
    }

    pub(crate) async fn upsert(&self, opts: UpsertOptions) -> Result<UpsertResult> {
        orchestrate_simple_crud(
            self.nmvb_handler.clone(),
            self.router.clone(),
            self.conn_manager.clone(),
            opts.key.clone().as_slice(),
            async |endpoint, vb_id, client| {
                client
                    .set(SetRequest {
                        collection_id: 0,
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
        orchestrate_simple_crud(
            self.nmvb_handler.clone(),
            self.router.clone(),
            self.conn_manager.clone(),
            opts.key.clone().as_slice(),
            async |endpoint, vb_id, client| {
                client
                    .get(GetRequest {
                        collection_id: 0,
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::cbconfig::TerseConfig;
    use crate::crudcomponent::CrudComponent;
    use crate::crudoptions::{GetOptions, UpsertOptions};
    use crate::kvclient::{KvClientConfig, StdKvClient};
    use crate::kvclientmanager::{
        KvClientManager, KvClientManagerConfig, KvClientManagerOptions, StdKvClientManager,
    };
    use crate::kvclientpool::NaiveKvClientPool;
    use crate::memdx::client::Client;
    use crate::memdx::packet::ResponsePacket;
    use crate::vbucketmap::VbucketMap;
    use crate::vbucketrouter::{
        NotMyVbucketConfigHandler, StdVbucketRouter, VbucketRouterOptions, VbucketRoutingInfo,
    };

    struct NVMBHandler {}

    impl NotMyVbucketConfigHandler for NVMBHandler {
        fn not_my_vbucket_config(&self, config: TerseConfig, source_hostname: &str) {}
    }

    struct Resp {}

    #[tokio::test]
    async fn can_orchestrate_memd_routing() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let (orphan_tx, mut orphan_rx) = unbounded_channel::<ResponsePacket>();

        tokio::spawn(async move {
            loop {
                match orphan_rx.recv().await {
                    Some(resp) => {
                        dbg!("unexpected orphan", resp);
                    }
                    None => {
                        return;
                    }
                }
            }
        });

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(PasswordAuthenticator {
                username: "Administrator".to_string(),
                password: "password".to_string(),
            })),
            selected_bucket: Some("default".to_string()),
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let mut client_configs = HashMap::new();
        client_configs.insert("192.168.107.128:11210".to_string(), client_config);

        let manger_config = KvClientManagerConfig {
            num_pool_connections: 1,
            clients: client_configs,
        };

        let manager: StdKvClientManager<NaiveKvClientPool<StdKvClient<Client>>> =
            StdKvClientManager::new(
                manger_config,
                KvClientManagerOptions {
                    connect_timeout: Default::default(),
                    connect_throttle_period: Default::default(),
                    orphan_handler: Arc::new(orphan_tx),
                },
            )
            .await
            .unwrap();

        let routing_info = VbucketRoutingInfo {
            vbucket_info: VbucketMap::new(
                vec![vec![0, 1], vec![1, 0], vec![0, 1], vec![0, 1], vec![1, 0]],
                1,
            )
            .unwrap(),
            server_list: vec!["192.168.107.128:11210".to_string()],
        };

        let dispatcher = StdVbucketRouter::new(routing_info, VbucketRouterOptions {});

        let dispatcher = Arc::new(dispatcher);
        let manager = Arc::new(manager);
        let nmvb_handler = Arc::new(NVMBHandler {});

        let crud_comp = CrudComponent::new(nmvb_handler, dispatcher, manager);
        let set_result = crud_comp
            .upsert(UpsertOptions {
                key: "test".into(),
                scope_name: None,
                collection_name: None,
                value: "value".into(),
                flags: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                durability_level: None,
            })
            .await
            .unwrap();

        dbg!(set_result);

        let get_result = crud_comp
            .get(GetOptions {
                key: "test".into(),
                scope_name: None,
                collection_name: None,
            })
            .await
            .unwrap();

        dbg!(get_result);
    }
}
