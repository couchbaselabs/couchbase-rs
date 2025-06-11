use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::bucket_client::{
    BucketClient, BucketClientBackend, Couchbase2BucketClient, CouchbaseBucketClient,
};
use crate::clients::bucket_mgmt_client::{
    BucketMgmtClient, BucketMgmtClientBackend, CouchbaseBucketMgmtClient,
};
use crate::clients::query_client::{CouchbaseQueryClient, QueryClient, QueryClientBackend};
use crate::clients::search_client::{CouchbaseSearchClient, SearchClient, SearchClientBackend};
use crate::clients::user_mgmt_client::{
    CouchbaseUserMgmtClient, UserMgmtClient, UserMgmtClientBackend,
};
use crate::error;
use crate::options::cluster_options::ClusterOptions;
use couchbase_connstr::{parse, resolve, Address, SrvRecord};
use couchbase_core::agentoptions::{CompressionConfig, SeedConfig};
use couchbase_core::ondemand_agentmanager::{OnDemandAgentManager, OnDemandAgentManagerOptions};
use couchbase_core::retry::RetryStrategy;
use couchbase_core::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use std::collections::HashMap;
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
        opts: ClusterOptions,
    ) -> error::Result<ClusterClient> {
        let conn_spec = parse(conn_str)?;
        let resolved_conn_spec = resolve(conn_spec).await?;

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

        let tls_config = if let Some(tls_config) = opts.tls_options {
            Some(
                tls_config
                    .try_into()
                    .map_err(|e| error::Error::other_failure(format!("{:?}", e)))?,
            )
        } else {
            None
        };

        let seed_config = SeedConfig::new()
            .memd_addrs(memd_hosts.iter().map(|a| a.to_string()).collect())
            .http_addrs(http_hosts.iter().map(|a| a.to_string()).collect());

        let mut core_opts: OnDemandAgentManagerOptions =
            OnDemandAgentManagerOptions::new(seed_config, opts.authenticator)
                .tls_config(tls_config);

        if let Some(timeout_opts) = opts.timeout_options {
            core_opts = core_opts.connect_timeout(timeout_opts.kv_connect_timeout);
        }

        if let Some(compression_mode) = opts.compression_mode {
            core_opts = core_opts.compression_config(CompressionConfig::new(compression_mode));
        }

        if core_opts.tls_config.is_some() && !use_ssl {
            return Err(error::Error::invalid_argument(
                "tls_config",
                "tls config provided but couchbase scheme used",
            ));
        } else if core_opts.tls_config.is_none() && use_ssl {
            return Err(error::Error::invalid_argument(
                "tls_config",
                "no TLS config provided but couchbases scheme used",
            ));
        }

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

    fn merge_options(
        opts: &mut OnDemandAgentManagerOptions,
        extra_opts: HashMap<String, Vec<String>>,
    ) -> error::Result<()> {
        for (k, v) in extra_opts {
            match k.as_str() {
                // TODO: This doesn't look complete...
                "kv_connect_timeout" => {
                    opts.connect_timeout =
                        Some(Duration::from_millis(v[0].parse().map_err(|e| {
                            error::Error::other_failure(format!("{:?}", e))
                        })?))
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
