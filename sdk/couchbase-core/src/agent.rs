use crate::address::Address;
use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::cbconfig::TerseConfig;
use crate::collection_resolver_cached::{
    CollectionResolverCached, CollectionResolverCachedOptions,
};
use crate::collection_resolver_memd::{CollectionResolverMemd, CollectionResolverMemdOptions};
use crate::compressionmanager::{CompressionManager, StdCompressor};
use crate::configmanager::{
    ConfigManager, ConfigManagerMemd, ConfigManagerMemdConfig, ConfigManagerMemdOptions,
};
use crate::configparser::ConfigParser;
use crate::crudcomponent::CrudComponent;
use crate::diagnosticscomponent::{DiagnosticsComponent, DiagnosticsComponentConfig};
use crate::errmapcomponent::ErrMapComponent;
use crate::error::{Error, ErrorKind, Result};
use crate::features::BucketFeature;
use crate::httpcomponent::HttpComponent;
use crate::httpx::client::{ClientConfig, ReqwestClient};
use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions, StdKvClient};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{
    KvClientManager, KvClientManagerConfig, KvClientManagerOptions, StdKvClientManager,
};
use crate::kvclientpool::{
    KvClientPool, KvClientPoolConfig, KvClientPoolOptions, NaiveKvClientPool,
};
use crate::memdx::client::Client;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::request::GetClusterConfigRequest;
use crate::mgmtcomponent::{MgmtComponent, MgmtComponentConfig, MgmtComponentOptions};
use crate::mgmtx::options::{GetTerseBucketConfigOptions, GetTerseClusterConfigOptions};
use crate::networktypeheuristic::NetworkTypeHeuristic;
use crate::nmvbhandler::{ConfigUpdater, StdNotMyVbucketConfigHandler};
use crate::options::agent::AgentOptions;
use crate::parsedconfig::{ParsedConfig, ParsedConfigBucketFeature, ParsedConfigFeature};
use crate::querycomponent::{QueryComponent, QueryComponentConfig, QueryComponentOptions};
use crate::retry::RetryManager;
use crate::searchcomponent::{SearchComponent, SearchComponentConfig, SearchComponentOptions};
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::util::{get_host_port_from_uri, get_hostname_from_host_port};
use crate::vbucketrouter::{
    StdVbucketRouter, VbucketRouter, VbucketRouterOptions, VbucketRoutingInfo,
};
use crate::{httpx, mgmtx};
use byteorder::BigEndian;
use futures::executor::block_on;
use log::{debug, error, info, warn};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::format;
use std::io::Cursor;
use std::net::ToSocketAddrs;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};
use uuid::Uuid;

#[derive(Clone)]
struct AgentState {
    bucket: Option<String>,
    tls_config: Option<TlsConfig>,
    authenticator: Arc<Authenticator>,
    auth_mechanisms: Vec<AuthMechanism>,
    num_pool_connections: usize,
    // http_transport:
    last_clients: HashMap<String, KvClientConfig>,
    latest_config: ParsedConfig,
    network_type: String,

    disable_mutation_tokens: bool,
    disable_server_durations: bool,
    kv_connect_timeout: Duration,
    kv_connect_throttle_timeout: Duration,
    http_idle_connection_timeout: Duration,
    http_max_idle_connections_per_host: Option<usize>,
    tcp_keep_alive_time: Duration,

    client_name: String,
}

type AgentClientManager = StdKvClientManager<NaiveKvClientPool<StdKvClient<Client>>>;
type AgentCollectionResolver = CollectionResolverCached<CollectionResolverMemd<AgentClientManager>>;

pub(crate) struct AgentInner {
    state: Arc<Mutex<AgentState>>,

    cfg_manager: Arc<ConfigManagerMemd<AgentClientManager>>,
    conn_mgr: Arc<AgentClientManager>,
    vb_router: Arc<StdVbucketRouter>,
    collections: Arc<AgentCollectionResolver>,
    retry_manager: Arc<RetryManager>,
    http_client: Arc<ReqwestClient>,
    err_map_component: Arc<ErrMapComponent>,

    pub(crate) crud: CrudComponent<
        AgentClientManager,
        StdVbucketRouter,
        StdNotMyVbucketConfigHandler<AgentInner>,
        AgentCollectionResolver,
        StdCompressor,
    >,

    pub(crate) query: Arc<QueryComponent<ReqwestClient>>,
    pub(crate) search: Arc<SearchComponent<ReqwestClient>>,
    pub(crate) mgmt: MgmtComponent<ReqwestClient>,
    pub(crate) diagnostics: DiagnosticsComponent<ReqwestClient, AgentClientManager>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct Agent {
    pub(crate) inner: Arc<AgentInner>,

    config_manager_shutdown_tx: Sender<()>,
}

struct AgentComponentConfigs {
    pub config_manager_memd_config: ConfigManagerMemdConfig,
    pub kv_client_manager_client_configs: HashMap<String, KvClientConfig>,
    pub vbucket_routing_info: VbucketRoutingInfo,
    pub query_config: QueryComponentConfig,
    pub search_config: SearchComponentConfig,
    pub mgmt_config: MgmtComponentConfig,
    pub diagnostics_config: DiagnosticsComponentConfig,
    pub http_client_config: ClientConfig,
}

impl AgentInner {
    pub async fn unsolicited_packet_handler(&self, packet: ResponsePacket) {
        if packet.op_code == OpCode::Set {
            if let Some(ref extras) = packet.extras {
                if extras.len() < 16 {
                    warn!("Received Set packet with too short extras: {packet:?}");
                    return;
                }

                let mut cursor = Cursor::new(extras);
                let server_rev_epoch = cursor.read_i64().await.unwrap();
                let server_rev_id = cursor.read_i64().await.unwrap();

                if let Some(config) = self
                    .cfg_manager
                    .out_of_band_version(server_rev_id, server_rev_epoch)
                    .await
                {
                    self.apply_config(config).await;
                }
            } else {
                warn!("Received Set packet with no extras: {packet:?}");
            }
        }
    }

    pub async fn apply_config(&self, config: ParsedConfig) {
        let mut state = self.state.lock().await;

        info!(
            "Agent applying updated config: rev_id={rev_id}, rev_epoch={rev_epoch}",
            rev_id = config.rev_id,
            rev_epoch = config.rev_epoch
        );
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
            error!("Failed to reconfigure connection manager (old clients); {e}");
        };

        self.vb_router
            .update_vbucket_info(agent_component_configs.vbucket_routing_info);

        if let Err(e) = self
            .cfg_manager
            .reconfigure(agent_component_configs.config_manager_memd_config)
        {
            error!("Failed to reconfigure memd config watcher component; {e}");
        }

        if let Err(e) = self
            .conn_mgr
            .reconfigure(KvClientManagerConfig {
                num_pool_connections: state.num_pool_connections,
                clients: agent_component_configs.kv_client_manager_client_configs,
            })
            .await
        {
            error!("Failed to reconfigure connection manager (updated clients); {e}");
        }

        if let Err(e) = self
            .http_client
            .reconfigure(agent_component_configs.http_client_config)
        {
            error!("Failed to reconfigure http client: {e}");
        }

        self.query.reconfigure(agent_component_configs.query_config);
        self.search
            .reconfigure(agent_component_configs.search_config);
        self.mgmt.reconfigure(agent_component_configs.mgmt_config);
        self.diagnostics
            .reconfigure(agent_component_configs.diagnostics_config);
    }

    fn gen_agent_component_configs(state: &mut AgentState) -> AgentComponentConfigs {
        let config = &state.latest_config;
        let rev_id = config.rev_id;
        let network_info = config.addresses_group_for_network_type(&state.network_type);

        let mut kv_data_node_ids = Vec::new();
        let mut kv_data_hosts: HashMap<String, Address> = HashMap::new();
        let mut mgmt_endpoints: HashMap<String, String> = HashMap::new();
        let mut query_endpoints: HashMap<String, String> = HashMap::new();
        let mut search_endpoints: HashMap<String, String> = HashMap::new();

        for node in network_info.nodes {
            let kv_ep_id = format!("kv{}", node.node_id);
            let mgmt_ep_id = format!("mgmt{}", node.node_id);
            let query_ep_id = format!("query{}", node.node_id);
            let search_ep_id = format!("search{}", node.node_id);

            if node.has_data {
                kv_data_node_ids.push(kv_ep_id.clone());
            }

            if state.tls_config.is_some() {
                if let Some(p) = node.ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        Address {
                            host: node.hostname.clone(),
                            port: p,
                        },
                    );
                }
                mgmt_endpoints.insert(
                    mgmt_ep_id,
                    format!("https://{}:{}", node.hostname, node.ssl_ports.mgmt),
                );
                if let Some(p) = node.ssl_ports.query {
                    query_endpoints.insert(query_ep_id, format!("https://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.ssl_ports.search {
                    search_endpoints
                        .insert(search_ep_id, format!("https://{}:{}", node.hostname, p));
                }
            } else {
                if let Some(p) = node.non_ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        Address {
                            host: node.hostname.clone(),
                            port: p,
                        },
                    );
                }
                mgmt_endpoints.insert(
                    mgmt_ep_id,
                    format!("http://{}:{}", node.hostname, node.non_ssl_ports.mgmt),
                );
                if let Some(p) = node.non_ssl_ports.query {
                    query_endpoints.insert(query_ep_id, format!("http://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.non_ssl_ports.search {
                    search_endpoints
                        .insert(search_ep_id, format!("http://{}:{}", node.hostname, p));
                }
            }
        }

        let mut clients = HashMap::new();
        for (node_id, address) in kv_data_hosts {
            let config = KvClientConfig {
                address,
                tls: state.tls_config.clone(),
                client_name: state.client_name.clone(),
                authenticator: state.authenticator.clone(),
                selected_bucket: state.bucket.clone(),
                disable_error_map: false,
                auth_mechanisms: state.auth_mechanisms.clone(),
                disable_mutation_tokens: state.disable_mutation_tokens,
                disable_server_durations: state.disable_server_durations,
                connect_timeout: state.kv_connect_timeout,
                tcp_keep_alive_time: state.tcp_keep_alive_time,
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

        let mut available_services = vec![ServiceType::MEMD];
        if !query_endpoints.is_empty() {
            available_services.push(ServiceType::QUERY)
        }
        if !search_endpoints.is_empty() {
            available_services.push(ServiceType::SEARCH)
        }

        AgentComponentConfigs {
            config_manager_memd_config: ConfigManagerMemdConfig {
                endpoints: kv_data_node_ids,
            },
            kv_client_manager_client_configs: clients,
            vbucket_routing_info,
            query_config: QueryComponentConfig {
                endpoints: query_endpoints,
                authenticator: state.authenticator.clone(),
            },
            search_config: SearchComponentConfig {
                endpoints: search_endpoints,
                authenticator: state.authenticator.clone(),
                vector_search_enabled: state
                    .latest_config
                    .features
                    .contains(&ParsedConfigFeature::FtsVectorSearch),
            },
            http_client_config: ClientConfig {
                tls_config: state.tls_config.clone(),
                idle_connection_timeout: state.http_idle_connection_timeout,
                max_idle_connections_per_host: state.http_max_idle_connections_per_host,
                tcp_keep_alive_time: state.tcp_keep_alive_time,
            },
            mgmt_config: MgmtComponentConfig {
                endpoints: mgmt_endpoints,
                authenticator: state.authenticator.clone(),
            },
            diagnostics_config: DiagnosticsComponentConfig {
                bucket: state.bucket.clone(),
                services: available_services,
                rev_id,
            },
        }
    }

    // TODO: This really shouldn't be async
    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        let guard = self.state.lock().await;

        if let Some(bucket) = &guard.latest_config.bucket {
            let mut features = vec![];

            for feature in &bucket.features {
                match feature {
                    ParsedConfigBucketFeature::CreateAsDeleted => {
                        features.push(BucketFeature::CreateAsDeleted)
                    }
                    ParsedConfigBucketFeature::ReplaceBodyWithXattr => {
                        features.push(BucketFeature::ReplaceBodyWithXattr)
                    }
                    ParsedConfigBucketFeature::RangeScan => features.push(BucketFeature::RangeScan),
                    ParsedConfigBucketFeature::ReplicaRead => {
                        features.push(BucketFeature::ReplicaRead)
                    }
                    ParsedConfigBucketFeature::NonDedupedHistory => {
                        features.push(BucketFeature::NonDedupedHistory)
                    }
                    ParsedConfigBucketFeature::ReviveDocument => {
                        features.push(BucketFeature::ReviveDocument)
                    }
                    _ => {}
                }
            }

            return Ok(features);
        }

        Err(ErrorKind::NoBucket.into())
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

        if let Some(config) = self.cfg_manager.out_of_band_config(parsed_config) {
            self.apply_config(config).await;
        };
    }
}

impl Agent {
    pub async fn new(opts: AgentOptions) -> Result<Self> {
        let build_version = env!("CARGO_PKG_VERSION");
        let client_name = format!("couchbase-rs-core {build_version}");
        info!("Creating new agent {client_name}");

        let auth_mechanisms = if opts.auth_mechanisms.is_empty() {
            if opts.tls_config.is_some() {
                vec![AuthMechanism::Plain]
            } else {
                vec![
                    AuthMechanism::ScramSha512,
                    AuthMechanism::ScramSha256,
                    AuthMechanism::ScramSha1,
                ]
            }
        } else {
            if opts.tls_config.is_none() && opts.auth_mechanisms.contains(&AuthMechanism::Plain) {
                warn!("PLAIN sends credentials in plaintext, this will cause credential leakage on the network");
            } else if opts.tls_config.is_some()
                && (opts.auth_mechanisms.contains(&AuthMechanism::ScramSha512)
                    || opts.auth_mechanisms.contains(&AuthMechanism::ScramSha256)
                    || opts.auth_mechanisms.contains(&AuthMechanism::ScramSha1))
            {
                warn!("Consider using PLAIN for TLS connections, as it is more efficient");
            }

            opts.auth_mechanisms
        };

        let mut state = AgentState {
            bucket: opts.bucket_name.clone(),
            authenticator: Arc::new(opts.authenticator),
            num_pool_connections: 1,
            last_clients: Default::default(),
            latest_config: ParsedConfig::default(),
            network_type: "".to_string(),
            client_name: client_name.clone(),
            tls_config: opts.tls_config,
            auth_mechanisms,
            disable_mutation_tokens: !opts.kv_config.enable_mutation_tokens,
            disable_server_durations: !opts.kv_config.enable_server_durations,
            kv_connect_timeout: opts.kv_config.connect_timeout,
            kv_connect_throttle_timeout: opts.kv_config.connect_throttle_timeout,
            http_idle_connection_timeout: opts.http_config.idle_connection_timeout,
            http_max_idle_connections_per_host: opts.http_config.max_idle_connections_per_host,
            tcp_keep_alive_time: opts
                .tcp_keep_alive_time
                .unwrap_or_else(|| Duration::from_secs(60)),
        };

        let http_client = Arc::new(ReqwestClient::new(ClientConfig {
            tls_config: state.tls_config.clone(),
            idle_connection_timeout: state.http_idle_connection_timeout,
            max_idle_connections_per_host: state.http_max_idle_connections_per_host,
            tcp_keep_alive_time: state.tcp_keep_alive_time,
        })?);

        let err_map_component = Arc::new(ErrMapComponent::new());

        let connect_timeout = opts.kv_config.connect_timeout;

        let first_kv_client_configs =
            Self::gen_first_kv_client_configs(&opts.seed_config.memd_addrs, &state);
        let first_http_client_configs =
            Self::gen_first_http_endpoints(&opts.seed_config.http_addrs, &state);
        let first_config = Self::get_first_config(
            first_kv_client_configs,
            first_http_client_configs,
            http_client.clone(),
            err_map_component.clone(),
            connect_timeout,
        )
        .await?;

        state.latest_config = first_config.clone();

        let network_type = NetworkTypeHeuristic::identify(&state.latest_config);
        state.network_type = network_type;

        let agent_component_configs = AgentInner::gen_agent_component_configs(&mut state);

        let err_map_component_conn_mgr = err_map_component.clone();

        let num_pool_connections = state.num_pool_connections;
        let state = Arc::new(Mutex::new(state));

        let (unsolicited_packet_tx, mut unsolicited_packet_rx) = mpsc::unbounded_channel();

        let conn_mgr = Arc::new(
            StdKvClientManager::new(
                KvClientManagerConfig {
                    num_pool_connections,
                    clients: agent_component_configs.kv_client_manager_client_configs,
                },
                KvClientManagerOptions {
                    connect_timeout,
                    connect_throttle_period: opts.kv_config.connect_throttle_timeout,
                    unsolicited_packet_tx: Some(unsolicited_packet_tx),
                    orphan_handler: Arc::new(|packet| {
                        info!("Orphan : {packet:?}");
                    }),
                    on_err_map_fetched_handler: Some(Arc::new(move |err_map| {
                        err_map_component_conn_mgr.on_err_map(err_map);
                    })),
                    disable_decompression: opts.compression_config.disable_decompression,
                },
            )
            .await?,
        );

        let (config_manager_shutdown_tx, config_manager_shutdown_rx) = broadcast::channel(1);

        let cfg_manager = Arc::new(ConfigManagerMemd::new(
            agent_component_configs.config_manager_memd_config,
            ConfigManagerMemdOptions {
                polling_period: opts.config_poller_config.poll_interval,
                kv_client_manager: conn_mgr.clone(),
                first_config,
                on_shutdown_rx: config_manager_shutdown_rx,
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

        let retry_manager = Arc::new(RetryManager::new(err_map_component.clone()));
        let compression_manager = Arc::new(CompressionManager::new(opts.compression_config));

        let crud = CrudComponent::new(
            nmvb_handler.clone(),
            vb_router.clone(),
            conn_mgr.clone(),
            collections.clone(),
            retry_manager.clone(),
            compression_manager,
        );

        let mgmt = MgmtComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            agent_component_configs.mgmt_config,
            MgmtComponentOptions {
                user_agent: client_name.clone(),
            },
        );

        let query = Arc::new(QueryComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            agent_component_configs.query_config,
            QueryComponentOptions {
                user_agent: client_name.clone(),
            },
        ));

        let search = Arc::new(SearchComponent::new(
            retry_manager.clone(),
            http_client.clone(),
            agent_component_configs.search_config,
            SearchComponentOptions {
                user_agent: client_name.clone(),
            },
        ));

        let diagnostics = DiagnosticsComponent::new(
            conn_mgr.clone(),
            query.clone(),
            search.clone(),
            retry_manager.clone(),
            agent_component_configs.diagnostics_config,
        );

        let inner = Arc::new(AgentInner {
            state,
            cfg_manager: cfg_manager.clone(),
            conn_mgr,
            vb_router,
            crud,
            collections,
            retry_manager,
            http_client,
            err_map_component,

            mgmt,
            query,
            search,
            diagnostics,
        });

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            while let Some(packet) = unsolicited_packet_rx.recv().await {
                inner_clone.unsolicited_packet_handler(packet).await;
            }
            debug!("Unsolicited packet handler exited");
        });

        nmvb_handler.set_watcher(inner.clone()).await;

        let agent = Agent {
            inner,
            config_manager_shutdown_tx,
        };

        agent.start_config_watcher(cfg_manager);

        Ok(agent)
    }

    fn start_config_watcher(&self, config_watcher: Arc<impl ConfigManager>) {
        let mut watch_rx = config_watcher.watch();

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

    async fn get_first_config<C: httpx::client::Client>(
        kv_client_manager_client_configs: HashMap<String, KvClientConfig>,
        http_configs: HashMap<String, FirstHttpConfig>,
        http_client: Arc<C>,
        err_map_component: Arc<ErrMapComponent>,
        connect_timeout: Duration,
    ) -> Result<ParsedConfig> {
        loop {
            for endpoint_config in kv_client_manager_client_configs.values() {
                let host = &endpoint_config.address;
                let err_map_component_clone = err_map_component.clone();
                let timeout_result = timeout(
                    connect_timeout,
                    StdKvClient::new(
                        endpoint_config.clone(),
                        KvClientOptions {
                            unsolicited_packet_tx: None,
                            orphan_handler: Arc::new(|packet| {}),
                            on_close: Arc::new(|id| {
                                Box::pin(async move {
                                    debug!("Bootstrap client {id} closed");
                                })
                            }),
                            on_err_map_fetched: Some(Arc::new(move |err_map| {
                                err_map_component_clone.on_err_map(err_map);
                            })),
                            disable_decompression: false,
                            id: Uuid::new_v4().to_string(),
                        },
                    ),
                )
                .await;

                let client: StdKvClient<Client> = match timeout_result {
                    Ok(client_result) => match client_result {
                        Ok(client) => client,
                        Err(e) => {
                            warn!("Failed to connect to endpoint: {e}");
                            continue;
                        }
                    },
                    Err(_e) => continue,
                };

                let raw_config = match client
                    .get_cluster_config(GetClusterConfigRequest {
                        known_version: None,
                    })
                    .await
                {
                    Ok(resp) => resp.config,
                    Err(_e) => continue,
                };

                client.close().await?;

                let config: TerseConfig =
                    serde_json::from_slice(raw_config.as_slice()).map_err(|e| {
                        Error::new_message_error(format!("failed to deserialize config: {e}"))
                    })?;

                match ConfigParser::parse_terse_config(config, host.host.as_str()) {
                    Ok(c) => {
                        return Ok(c);
                    }
                    Err(_e) => continue,
                };
            }

            info!("Failed to fetch config over kv, attempting http");
            for endpoint_config in http_configs.values() {
                let endpoint = endpoint_config.endpoint.clone();
                let host = get_host_port_from_uri(&endpoint)?;
                let user_pass = match endpoint_config.authenticator.as_ref() {
                    Authenticator::PasswordAuthenticator(authenticator) => {
                        authenticator.get_credentials(&ServiceType::MGMT, host)?
                    }
                    Authenticator::CertificateAuthenticator(authenticator) => {
                        authenticator.get_credentials(&ServiceType::MGMT, host)?
                    }
                };

                match Self::fetch_http_config(
                    http_client.clone(),
                    endpoint,
                    endpoint_config.user_agent.clone(),
                    user_pass.username,
                    user_pass.password,
                    endpoint_config.bucket_name.clone(),
                )
                .await
                {
                    Ok(c) => {
                        return Ok(c);
                    }
                    Err(_e) => {}
                };
            }

            info!("Failed to fetch config from any source");

            // TODO: Make configurable?
            sleep(Duration::from_secs(1)).await;
        }
    }

    pub(crate) async fn fetch_http_config<C: httpx::client::Client>(
        http_client: Arc<C>,
        endpoint: String,
        user_agent: String,
        username: String,
        password: String,
        bucket_name: Option<String>,
    ) -> Result<ParsedConfig> {
        debug!("Polling config from {}", &endpoint);

        let host_port = get_host_port_from_uri(&endpoint)?;
        let hostname = get_hostname_from_host_port(&host_port)?;

        let parsed = if let Some(bucket_name) = bucket_name {
            let config = mgmtx::mgmt::Management {
                http_client,
                user_agent,
                endpoint,
                username,
                password,
            }
            .get_terse_bucket_config(&GetTerseBucketConfigOptions {
                bucket_name: &bucket_name,
                on_behalf_of_info: None,
            })
            .await
            .map_err(Error::from)?;

            ConfigParser::parse_terse_config(config, &hostname)?
        } else {
            let config = mgmtx::mgmt::Management {
                http_client,
                user_agent,
                endpoint,
                username,
                password,
            }
            .get_terse_cluster_config(&GetTerseClusterConfigOptions {
                on_behalf_of_info: None,
            })
            .await
            .map_err(Error::from)?;

            ConfigParser::parse_terse_config(config, &hostname)?
        };

        Ok(parsed)
    }

    fn gen_first_kv_client_configs(
        memd_addrs: &Vec<Address>,
        state: &AgentState,
    ) -> HashMap<String, KvClientConfig> {
        let mut clients = HashMap::new();
        for addr in memd_addrs {
            let node_id = format!("kv{addr}");
            let config = KvClientConfig {
                address: addr.clone(),
                tls: state.tls_config.clone(),
                client_name: state.client_name.clone(),
                authenticator: state.authenticator.clone(),
                selected_bucket: state.bucket.clone(),
                disable_error_map: false,
                auth_mechanisms: state.auth_mechanisms.clone(),
                disable_mutation_tokens: state.disable_mutation_tokens,
                disable_server_durations: state.disable_server_durations,
                connect_timeout: state.kv_connect_timeout,
                tcp_keep_alive_time: state.tcp_keep_alive_time,
            };
            clients.insert(node_id, config);
        }

        clients
    }

    fn gen_first_http_endpoints(
        mgmt_addrs: &Vec<Address>,
        state: &AgentState,
    ) -> HashMap<String, FirstHttpConfig> {
        let mut clients = HashMap::new();
        for addr in mgmt_addrs {
            let node_id = format!("mgmt{addr}");
            let base = if state.tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let config = FirstHttpConfig {
                endpoint: format!("{base}://{addr}"),
                tls: state.tls_config.clone(),
                user_agent: state.client_name.clone(),
                authenticator: state.authenticator.clone(),
                bucket_name: state.bucket.clone(),
            };
            clients.insert(node_id, config);
        }

        clients
    }

    pub async fn close(&mut self) {
        self.config_manager_shutdown_tx.send(()).unwrap_or_default();

        self.inner.conn_mgr.close().await.unwrap_or_default();
    }
}

struct FirstHttpConfig {
    pub endpoint: String,
    pub tls: Option<TlsConfig>,
    pub user_agent: String,
    pub authenticator: Arc<Authenticator>,
    pub bucket_name: Option<String>,
}
