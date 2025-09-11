use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::bucket_client::{
    BucketClient, BucketClientBackend, Couchbase2BucketClient, CouchbaseBucketClient,
};
use crate::clients::bucket_mgmt_client::{
    BucketMgmtClient, BucketMgmtClientBackend, CouchbaseBucketMgmtClient,
};
use crate::clients::diagnostics_client::{
    CouchbaseDiagnosticsClient, DiagnosticsClient, DiagnosticsClientBackend,
};
use crate::clients::query_client::{CouchbaseQueryClient, QueryClient, QueryClientBackend};
use crate::clients::search_client::{CouchbaseSearchClient, SearchClient, SearchClientBackend};
use crate::clients::user_mgmt_client::{
    CouchbaseUserMgmtClient, UserMgmtClient, UserMgmtClientBackend,
};
use crate::error;
use crate::options::cluster_options::ClusterOptions;
use couchbase_connstr::{parse, resolve, Address, SrvRecord};
use couchbase_core::address;
use couchbase_core::ondemand_agentmanager::OnDemandAgentManager;
use couchbase_core::options::agent::{CompressionConfig, SeedConfig};
use couchbase_core::options::ondemand_agentmanager::OnDemandAgentManagerOptions;
use couchbase_core::options::orphan_reporter::OrphanReporterConfig;
use couchbase_core::orphan_reporter::OrphanReporter;
use couchbase_core::retry::RetryStrategy;
use couchbase_core::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use std::collections::HashMap;
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct ClusterClient {
    backend: ClusterClientBackend,
}

enum ClusterClientBackend {
    CouchbaseClusterBackend(CouchbaseClusterBackend),
    Couchbase2ClusterBackend(Couchbase2ClusterBackend),
}

impl ClusterClient {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        mut opts: ClusterOptions,
    ) -> error::Result<ClusterClient> {
        let conn_spec = parse(conn_str)?;

        // This isn't ideal but dns options have to be a part of ClusterOptions, and we need to pull
        // the dns options out for resolve.
        // We could create a new type to pass into the backend connect functions but it just
        // seems unnecessary.
        let dns_options = take(&mut opts.dns_options);
        let resolved_conn_spec = resolve(
            conn_spec,
            dns_options.map(couchbase_connstr::DnsConfig::from),
        )
        .await?;

        let backend = if let Some(host) = resolved_conn_spec.couchbase2_host {
            ClusterClientBackend::Couchbase2ClusterBackend(
                Couchbase2ClusterBackend::connect(host, opts, resolved_conn_spec.options).await?,
            )
        } else {
            ClusterClientBackend::CouchbaseClusterBackend(
                CouchbaseClusterBackend::connect(
                    resolved_conn_spec.memd_hosts,
                    resolved_conn_spec.http_hosts,
                    resolved_conn_spec.srv_record,
                    resolved_conn_spec.use_ssl,
                    opts,
                    resolved_conn_spec.options,
                )
                .await?,
            )
        };

        Ok(ClusterClient { backend })
    }

    pub fn bucket_client(&self, name: String) -> BucketClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let bucket_client = backend.bucket(name);

                BucketClient::new(BucketClientBackend::CouchbaseBucketBackend(bucket_client))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(backend) => {
                let bucket_client = backend.bucket(name);

                BucketClient::new(BucketClientBackend::Couchbase2BucketBackend(bucket_client))
            }
        }
    }

    pub fn buckets_client(&self) -> BucketMgmtClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let bucket_client = backend.buckets();

                BucketMgmtClient::new(BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(
                    bucket_client,
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_backend) => {
                unimplemented!()
            }
        }
    }

    pub fn users_client(&self) -> UserMgmtClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let user_client = backend.users();

                UserMgmtClient::new(UserMgmtClientBackend::CouchbaseUserMgmtClientBackend(
                    user_client,
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_backend) => {
                unimplemented!()
            }
        }
    }

    pub fn query_client(&self) -> QueryClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let query_client = backend.query_client();

                QueryClient::new(QueryClientBackend::CouchbaseQueryClientBackend(
                    query_client,
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn search_client(&self) -> SearchClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let search_client = backend.search_client();

                SearchClient::new(SearchClientBackend::CouchbaseSearchClientBackend(
                    search_client,
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn diagnostics_client(&self) -> DiagnosticsClient {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let diagnostics_client = backend.diagnostics_client();

                DiagnosticsClient::new(DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(
                    diagnostics_client,
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_) => {
                unimplemented!()
            }
        }
    }
}

struct CouchbaseClusterBackend {
    agent_manager: Arc<OnDemandAgentManager>,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseClusterBackend {
    pub async fn connect(
        memd_hosts: Vec<Address>,
        http_hosts: Vec<Address>,
        _srv_record: Option<SrvRecord>,
        use_ssl: bool,
        opts: ClusterOptions,
        extra_opts: HashMap<String, Vec<String>>,
    ) -> error::Result<CouchbaseClusterBackend> {
        let default_retry_strategy = Arc::new(BestEffortRetryStrategy::new(
            ExponentialBackoffCalculator::default(),
        ));

        let tls_config = if let Some(tls_options) = opts.tls_options {
            let tls_config = tls_options.try_into_tls_config(&opts.authenticator)?;

            Some(tls_config)
        } else {
            None
        };

        let seed_config = SeedConfig::new()
            .memd_addrs(
                memd_hosts
                    .into_iter()
                    .map(|a| address::Address {
                        host: a.host,
                        port: a.port,
                    })
                    .collect(),
            )
            .http_addrs(
                http_hosts
                    .into_iter()
                    .map(|a| address::Address {
                        host: a.host,
                        port: a.port,
                    })
                    .collect(),
            );

        let mut compression_config = CompressionConfig::default();
        if let Some(cm) = opts.compression_mode {
            compression_config = compression_config.mode(cm.into());
        }

        if tls_config.is_some() && !use_ssl {
            return Err(error::Error::invalid_argument(
                "tls_config",
                "tls config provided but couchbase scheme used",
            ));
        } else if tls_config.is_none() && use_ssl {
            return Err(error::Error::invalid_argument(
                "tls_config",
                "no TLS config provided but couchbases scheme used",
            ));
        }

        let orphan_handler = if opts.orphan_reporter_options.enabled.unwrap_or(true) {
            let cfg: OrphanReporterConfig = opts.orphan_reporter_options.into();
            Some(OrphanReporter::new(cfg).get_handle())
        } else {
            None
        };

        let mut core_opts =
            OnDemandAgentManagerOptions::new(seed_config, opts.authenticator.into())
                .tls_config(tls_config)
                .kv_config(opts.kv_options.into())
                .http_config(opts.http_options.into())
                .auth_mechanisms(vec![])
                .compression_config(compression_config)
                .config_poller_config(opts.poller_options.into())
                .tcp_keep_alive_time(
                    opts.tcp_keep_alive_time
                        .unwrap_or_else(|| Duration::from_secs(60)),
                )
                .orphan_reporter_handler(orphan_handler);

        Self::merge_options(&mut core_opts, extra_opts)?;

        let mgr = OnDemandAgentManager::new(core_opts).await?;

        Ok(Self {
            agent_manager: Arc::new(mgr),
            default_retry_strategy,
        })
    }

    fn bucket(&self, name: String) -> CouchbaseBucketClient {
        CouchbaseBucketClient::new(
            CouchbaseAgentProvider::with_bucket(self.agent_manager.clone(), name.clone()),
            name,
            self.default_retry_strategy.clone(),
        )
    }

    fn buckets(&self) -> CouchbaseBucketMgmtClient {
        let agent = self.agent_manager.get_cluster_agent();

        CouchbaseBucketMgmtClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
            self.default_retry_strategy.clone(),
        )
    }

    fn users(&self) -> CouchbaseUserMgmtClient {
        let agent = self.agent_manager.get_cluster_agent();

        CouchbaseUserMgmtClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
            self.default_retry_strategy.clone(),
        )
    }

    fn query_client(&self) -> CouchbaseQueryClient {
        let agent = self.agent_manager.get_cluster_agent();

        CouchbaseQueryClient::new(CouchbaseAgentProvider::with_agent(agent.clone()))
    }

    fn search_client(&self) -> CouchbaseSearchClient {
        let agent = self.agent_manager.get_cluster_agent();

        CouchbaseSearchClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
            self.default_retry_strategy.clone(),
        )
    }

    fn diagnostics_client(&self) -> CouchbaseDiagnosticsClient {
        let agent = self.agent_manager.get_cluster_agent();

        CouchbaseDiagnosticsClient::new(CouchbaseAgentProvider::with_agent(agent.clone()))
    }

    fn merge_options(
        opts: &mut OnDemandAgentManagerOptions,
        extra_opts: HashMap<String, Vec<String>>,
    ) -> error::Result<()> {
        for (k, v) in extra_opts {
            match k.as_str() {
                "kv_connect_timeout" => {
                    opts.kv_config.connect_timeout = Duration::from_millis(
                        v[0].parse()
                            .map_err(|e| error::Error::other_failure(format!("{e:?}")))?,
                    )
                }
                "enable_tls" => {
                    let enabled: bool = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;

                    if !enabled {
                        opts.tls_config = None;
                    }

                    if enabled && opts.tls_config.is_none() {
                        return Err(error::Error::invalid_argument(
                            "enable_tls",
                            "enable_tls is true but no tls_config provided",
                        ));
                    }
                }
                "enable_mutation_tokens" => {
                    let enabled = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.kv_config.enable_mutation_tokens = enabled;
                }
                "enable_server_durations" => {
                    let enabled = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.kv_config.enable_server_durations = enabled;
                }
                "tcp_keep_alive_time" => {
                    let duration = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.tcp_keep_alive_time = Some(Duration::from_millis(duration));
                }
                "config_poll_interval" => {
                    let interval = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.config_poller_config.poll_interval = Duration::from_millis(interval);
                }
                "num_kv_connections" => {
                    let num_connections = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.kv_config.num_connections = num_connections;
                }
                "max_idle_http_connections_per_host" => {
                    let max_idle_http_connections = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.http_config.max_idle_connections_per_host =
                        Some(max_idle_http_connections);
                }
                "idle_http_connection_timeout" => {
                    let idle_http_connection_timeout = v[0]
                        .parse()
                        .map_err(|e| error::Error::other_failure(format!("{e:?}")))?;
                    opts.http_config.idle_connection_timeout =
                        Duration::from_millis(idle_http_connection_timeout);
                }
                "placeholder" => {}
                _ => (),
            }
        }

        Ok(())
    }
}

struct Couchbase2ClusterBackend {}

impl Couchbase2ClusterBackend {
    pub async fn connect(
        _host: Address,
        _opts: ClusterOptions,
        _extra_opts: HashMap<String, Vec<String>>,
    ) -> error::Result<Couchbase2ClusterBackend> {
        unimplemented!()
    }

    fn bucket(&self, name: String) -> Couchbase2BucketClient {
        Couchbase2BucketClient::new(name)
    }
}
