use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use futures::executor::block_on;
use log::{debug, error, info};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout, timeout_at};

use crate::agentoptions::AgentOptions;
use crate::authenticator::Authenticator;
use crate::cbconfig::TerseConfig;
use crate::collection_resolver_cached::{
    CollectionResolverCached, CollectionResolverCachedOptions,
};
use crate::collection_resolver_memd::{CollectionResolverMemd, CollectionResolverMemdOptions};
use crate::compressionmanager::{CompressionManager, StdCompressor};
use crate::configparser::ConfigParser;
use crate::configwatcher::{
    ConfigWatcher, ConfigWatcherMemd, ConfigWatcherMemdConfig, ConfigWatcherMemdOptions,
};
use crate::crudcomponent::CrudComponent;
use crate::error::Result;
use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions, StdKvClient};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{
    KvClientManager, KvClientManagerConfig, KvClientManagerOptions, StdKvClientManager,
};
use crate::kvclientpool::{
    KvClientPool, KvClientPoolConfig, KvClientPoolOptions, NaiveKvClientPool,
};
use crate::memdx::client::Client;
use crate::memdx::connection::TlsConfig;
use crate::memdx::request::GetClusterConfigRequest;
use crate::networktypeheuristic::NetworkTypeHeuristic;
use crate::nmvbhandler::{ConfigUpdater, StdNotMyVbucketConfigHandler};
use crate::parsedconfig::ParsedConfig;
use crate::retry::RetryManager;
use crate::vbucketrouter::{
    StdVbucketRouter, VbucketRouter, VbucketRouterOptions, VbucketRoutingInfo,
};

#[derive(Debug, Clone)]
struct AgentState {
    bucket: Option<String>,
    tls_config: Option<TlsConfig>,
    authenticator: Option<Arc<Authenticator>>,
    num_pool_connections: usize,
    // http_transport:
    last_clients: HashMap<String, KvClientConfig>,
    latest_config: ParsedConfig,
    network_type: String,

    client_name: String,
}

type AgentClientManager = StdKvClientManager<NaiveKvClientPool<StdKvClient<Client>>>;
type AgentCollectionResolver = CollectionResolverCached<CollectionResolverMemd<AgentClientManager>>;

pub(crate) struct AgentInner {
    state: Arc<Mutex<AgentState>>,

    cfg_watcher: Arc<dyn ConfigWatcher>,
    conn_mgr: Arc<AgentClientManager>,
    vb_router: Arc<StdVbucketRouter>,
    collections: Arc<AgentCollectionResolver>,
    retry_manager: Arc<RetryManager>,

    pub crud: CrudComponent<
        AgentClientManager,
        StdVbucketRouter,
        StdNotMyVbucketConfigHandler<AgentInner>,
        AgentCollectionResolver,
        StdCompressor,
    >,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct Agent {
    pub(crate) inner: Arc<AgentInner>,

    config_watcher_shutdown_tx: Sender<()>,
}

#[derive(Debug, Clone)]
struct AgentComponentConfigs {
    pub config_watcher_memd_config: ConfigWatcherMemdConfig,
    pub kv_client_manager_client_configs: HashMap<String, KvClientConfig>,
    pub vbucket_routing_info: VbucketRoutingInfo,
}

impl AgentInner {
    pub async fn apply_config(&self, config: ParsedConfig) {
        let mut state = self.state.lock().await;

        if !Self::can_update_config(&config, &state.latest_config) {
            return;
        }

        info!("Applying updated config");
        state.latest_config = config;

        self.update_state(&mut state).await;
    }

    async fn update_state(&self, state: &mut AgentState) {
        let agent_component_configs = Self::gen_agent_component_configs(state);

        // In order to avoid race conditions between operations selecting the
        // endpoint they need to send the request to, and fetching an actual
        // client which can send to that endpoint.  We must first ensure that
        // all the new endpoints are available in the manager.  Then update
        // the routing table.  Then go back and remove the old entries from
        // the connection manager list.

        let mut old_clients = HashMap::new();
        for (client_name, client) in &state.last_clients {
            old_clients.insert(client_name.clone(), client.clone());
        }

        for (client_name, client) in &agent_component_configs.kv_client_manager_client_configs {
            old_clients
                .entry(client_name.clone())
                .or_insert(client.clone());
        }

        if let Err(e) = self
            .conn_mgr
            .reconfigure(KvClientManagerConfig {
                num_pool_connections: state.num_pool_connections,
                clients: old_clients,
            })
            .await
        {
            error!(
                "Failed to reconfigure connection manager (old clients); {}",
                e.to_string()
            );
        };

        self.vb_router
            .update_vbucket_info(agent_component_configs.vbucket_routing_info);

        if let Err(e) = self
            .cfg_watcher
            .reconfigure(agent_component_configs.config_watcher_memd_config)
        {
            error!(
                "Failed to reconfigure memd config watcher component; {}",
                e.to_string()
            );
        }

        if let Err(e) = self
            .conn_mgr
            .reconfigure(KvClientManagerConfig {
                num_pool_connections: state.num_pool_connections,
                clients: agent_component_configs.kv_client_manager_client_configs,
            })
            .await
        {
            error!(
                "Failed to reconfigure connection manager (updated clients); {}",
                e.to_string()
            );
        }
    }

    fn can_update_config(new_config: &ParsedConfig, old_config: &ParsedConfig) -> bool {
        if new_config.bucket != old_config.bucket {
            debug!("Switching config due to changed bucket type (bucket takeover)");
            return true;
        } else if let Some(cmp) = new_config.partial_cmp(old_config) {
            if cmp == Ordering::Less {
                debug!("Skipping config due to new config being an older revision")
            } else if cmp == Ordering::Equal {
                debug!("Skipping config due to matching revisions")
            } else {
                return true;
            }
        }

        false
    }

    fn gen_agent_component_configs(state: &mut AgentState) -> AgentComponentConfigs {
        let network_info = state
            .latest_config
            .addresses_group_for_network_type(&state.network_type);

        let mut kv_data_node_ids = Vec::with_capacity(network_info.nodes.len());
        let mut kv_data_hosts: HashMap<String, String> =
            HashMap::with_capacity(network_info.nodes.len());

        // let tls_config = state.tls_config;

        for node in network_info.nodes {
            let kv_ep_id = format!("kv{}", node.node_id);

            if node.has_data {
                kv_data_node_ids.push(kv_ep_id.clone());
            }

            if state.tls_config.is_some() {
                if let Some(p) = node.ssl_ports.kv {
                    kv_data_hosts.insert(kv_ep_id, format!("{}:{}", node.hostname, p));
                }
            } else if let Some(p) = node.non_ssl_ports.kv {
                kv_data_hosts.insert(kv_ep_id, format!("{}:{}", node.hostname, p));
            }
        }

        let mut clients = HashMap::new();
        for (node_id, addr) in kv_data_hosts {
            let config = KvClientConfig {
                // TODO: unwrap, return error on fail?
                address: addr.parse().unwrap(),
                tls: state.tls_config.clone(),
                client_name: state.client_name.clone(),
                authenticator: state.authenticator.clone(),
                selected_bucket: state.bucket.clone(),
                disable_default_features: false,
                disable_error_map: false,
                disable_bootstrap: false,
            };
            clients.insert(node_id, config);
        }

        let vbucket_routing_info = if let Some(info) = &state.latest_config.bucket {
            VbucketRoutingInfo {
                // TODO: Clone
                vbucket_info: info.vbucket_map.clone(),
                server_list: kv_data_node_ids.clone(),
                bucket_selected: state.bucket.is_some(),
            }
        } else {
            VbucketRoutingInfo {
                vbucket_info: None,
                server_list: kv_data_node_ids.clone(),
                bucket_selected: state.bucket.is_some(),
            }
        };

        AgentComponentConfigs {
            config_watcher_memd_config: ConfigWatcherMemdConfig {
                endpoints: kv_data_node_ids,
            },
            kv_client_manager_client_configs: clients,
            vbucket_routing_info,
        }
    }
}

impl ConfigUpdater for AgentInner {
    async fn apply_terse_config(&self, config: TerseConfig, source_hostname: &str) {
        let parsed_config = match ConfigParser::parse_terse_config(config, source_hostname) {
            Ok(cfg) => cfg,
            Err(_e) => {
                // TODO: log
                return;
            }
        };

        self.apply_config(parsed_config).await;
    }
}

impl Agent {
    pub async fn new(opts: AgentOptions) -> Result<Self> {
        let build_version = env!("CARGO_PKG_VERSION");
        let client_name = format!("couchbase-rs-core {}", build_version);

        let authenticator = opts.authenticator.map(Arc::new);

        let mut state = AgentState {
            bucket: opts.bucket_name,
            authenticator,
            num_pool_connections: 1,
            last_clients: Default::default(),
            latest_config: ParsedConfig::default(),
            network_type: "".to_string(),
            client_name,
            tls_config: opts.tls_config.map(|cfg| cfg.into()),
        };

        let connect_timeout = opts.connect_timeout.unwrap_or(Duration::from_secs(7));
        let connect_throttle_period = opts.connect_timeout.unwrap_or(Duration::from_secs(5));

        let first_kv_client_configs =
            Self::gen_first_kv_client_configs(&opts.seed_config.memd_addrs, &state);
        let first_config = Self::get_first_config(first_kv_client_configs, connect_timeout).await?;

        state.latest_config = first_config;

        let network_type = NetworkTypeHeuristic::identify(&state.latest_config);
        state.network_type = network_type;

        let agent_component_configs = AgentInner::gen_agent_component_configs(&mut state);

        let conn_mgr = Arc::new(
            StdKvClientManager::new(
                KvClientManagerConfig {
                    num_pool_connections: state.num_pool_connections,
                    clients: agent_component_configs.kv_client_manager_client_configs,
                },
                KvClientManagerOptions {
                    connect_timeout,
                    connect_throttle_period,
                    orphan_handler: Arc::new(|packet| {
                        info!("Orphan : {:?}", packet);
                    }),
                    disable_decompression: opts.compression_config.disable_decompression,
                },
            )
            .await?,
        );

        let cfg_watcher = Arc::new(ConfigWatcherMemd::new(
            agent_component_configs.config_watcher_memd_config,
            ConfigWatcherMemdOptions {
                polling_period: Duration::from_millis(2500),
                kv_client_manager: conn_mgr.clone(),
            },
        ));
        let vb_router = Arc::new(StdVbucketRouter::new(
            agent_component_configs.vbucket_routing_info,
            VbucketRouterOptions {},
        ));

        let nmvb_handler = Arc::new(StdNotMyVbucketConfigHandler::new());

        let memd_resolver = CollectionResolverMemd::new(CollectionResolverMemdOptions {
            conn_mgr: conn_mgr.clone(),
        });

        let collections = Arc::new(CollectionResolverCached::new(
            CollectionResolverCachedOptions {
                resolver: memd_resolver,
            },
        ));

        let retry_manager = Arc::new(RetryManager::default());
        let compression_manager = Arc::new(CompressionManager::new(opts.compression_config));

        let crud = CrudComponent::new(
            nmvb_handler.clone(),
            vb_router.clone(),
            conn_mgr.clone(),
            collections.clone(),
            retry_manager.clone(),
            compression_manager,
        );
        let inner = Arc::new(AgentInner {
            state: Arc::new(Mutex::new(state)),
            cfg_watcher: cfg_watcher.clone(),
            conn_mgr,
            vb_router,
            crud,
            collections,
            retry_manager,
        });

        nmvb_handler.set_watcher(inner.clone()).await;

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let agent = Agent {
            inner,
            config_watcher_shutdown_tx: shutdown_tx,
        };

        agent.start_config_watcher(cfg_watcher, shutdown_rx);

        Ok(agent)
    }

    fn start_config_watcher(
        &self,
        config_watcher: Arc<impl ConfigWatcher>,
        shutdown_rx: Receiver<()>,
    ) {
        let mut watch_rx = config_watcher.watch(shutdown_rx);

        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                match watch_rx.recv().await {
                    Ok(pc) => {
                        inner.apply_config(pc).await;
                    }
                    Err(_e) => {
                        return;
                    }
                }
            }
        });
    }

    async fn get_first_config(
        kv_client_manager_client_configs: HashMap<String, KvClientConfig>,
        connect_timeout: Duration,
    ) -> Result<ParsedConfig> {
        loop {
            for endpoint_config in kv_client_manager_client_configs.values() {
                let host = endpoint_config.address.ip();
                let timeout_result = timeout(
                    connect_timeout,
                    StdKvClient::new(
                        endpoint_config.clone(),
                        KvClientOptions {
                            orphan_handler: Arc::new(|packet| {}),
                            on_close: Arc::new(|id| {
                                Box::pin(async move {
                                    debug!("Bootstrap client {} closed", id);
                                })
                            }),
                            disable_decompression: false,
                        },
                    ),
                )
                .await;

                let client: StdKvClient<Client> = match timeout_result {
                    Ok(client_result) => match client_result {
                        Ok(client) => client,
                        Err(_e) => continue,
                    },
                    Err(_e) => continue,
                };

                let raw_config = match client.get_cluster_config(GetClusterConfigRequest {}).await {
                    Ok(resp) => resp.config,
                    Err(_e) => continue,
                };

                client.close().await.unwrap();

                let config: TerseConfig = serde_json::from_slice(raw_config.as_slice())?;

                match ConfigParser::parse_terse_config(config, &host.to_string()) {
                    Ok(c) => {
                        return Ok(c);
                    }
                    Err(_e) => continue,
                };
            }
        }
    }

    fn gen_first_kv_client_configs(
        memd_addrs: &Vec<String>,
        state: &AgentState,
    ) -> HashMap<String, KvClientConfig> {
        let mut clients = HashMap::new();
        for addr in memd_addrs {
            let node_id = format!("kv{}", addr);
            let config = KvClientConfig {
                // TODO: unwrap, return error on fail?
                address: addr.parse().unwrap(),
                tls: state.tls_config.clone(),
                client_name: state.client_name.clone(),
                authenticator: state.authenticator.clone(),
                selected_bucket: state.bucket.clone(),
                disable_default_features: false,
                disable_error_map: false,
                disable_bootstrap: false,
            };
            clients.insert(node_id, config);
        }

        clients
    }

    pub async fn close(&mut self) {
        self.config_watcher_shutdown_tx.send(()).unwrap_or_default();

        self.inner.conn_mgr.close().await.unwrap_or_default();
    }
}

// impl Drop for Agent {
//     fn drop(&mut self) {
//         self.config_watcher_shutdown_tx.send(()).unwrap_or_default();
//
//         block_on(async { self.inner.conn_mgr.close().await }).unwrap_or_default();
//     }
// }
