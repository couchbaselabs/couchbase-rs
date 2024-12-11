use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::analytics_client::{
    AnalyticsClient, AnalyticsClientBackend, CouchbaseAnalyticsClient,
};
use crate::clients::bucket_client::{
    BucketClient, BucketClientBackend, Couchbase2BucketClient, CouchbaseBucketClient,
};
use crate::clients::query_client::{CouchbaseQueryClient, QueryClient, QueryClientBackend};
use crate::clients::search_client::{CouchbaseSearchClient, SearchClient, SearchClientBackend};
use crate::error;
use crate::options::cluster_options::ClusterOptions;
use crate::retry::DEFAULT_RETRY_STRATEGY;
use couchbase_connstr::{parse, resolve, Address, SrvRecord};
use couchbase_core::ondemand_agentmanager::{OnDemandAgentManager, OnDemandAgentManagerOptions};
use couchbase_core::retry::RetryStrategy;
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

    pub fn query_client(&self) -> error::Result<QueryClient> {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let query_client = backend.query_client()?;

                Ok(QueryClient::new(
                    QueryClientBackend::CouchbaseQueryClientBackend(query_client),
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn search_client(&self) -> error::Result<SearchClient> {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let search_client = backend.search_client()?;

                Ok(SearchClient::new(
                    SearchClientBackend::CouchbaseSearchClientBackend(search_client),
                ))
            }
            ClusterClientBackend::Couchbase2ClusterBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn analytics_client(&self) -> error::Result<AnalyticsClient> {
        match &self.backend {
            ClusterClientBackend::CouchbaseClusterBackend(backend) => {
                let analytics_client = backend.analytics_client()?;

                Ok(AnalyticsClient::new(
                    AnalyticsClientBackend::CouchbaseAnalyticsClientBackend(analytics_client),
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
        let default_retry_strategy = match &opts.retry_strategy {
            Some(r) => r.clone(),
            None => DEFAULT_RETRY_STRATEGY.clone(),
        };

        let mut opts: OnDemandAgentManagerOptions = opts.try_into()?;

        if opts.tls_config.is_some() && !use_ssl {
            return Err(error::Error {
                msg: "TLS config provided but couchbase scheme used".into(),
            });
        } else if opts.tls_config.is_none() && use_ssl {
            return Err(error::Error {
                msg: "No TLS config provided but couchbases scheme used".into(),
            });
        }

        Self::merge_options(&mut opts, extra_opts)?;

        opts.seed_config.memd_addrs = memd_hosts.iter().map(|a| a.to_string()).collect();
        opts.seed_config.http_addrs = http_hosts.iter().map(|a| a.to_string()).collect();

        let mgr = OnDemandAgentManager::new(opts).await?;

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

    fn query_client(&self) -> error::Result<CouchbaseQueryClient> {
        let agent = self.agent_manager.get_cluster_agent()?;

        Ok(CouchbaseQueryClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
        ))
    }

    fn search_client(&self) -> error::Result<CouchbaseSearchClient> {
        let agent = self.agent_manager.get_cluster_agent()?;

        Ok(CouchbaseSearchClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
        ))
    }

    fn analytics_client(&self) -> error::Result<CouchbaseAnalyticsClient> {
        let agent = self.agent_manager.get_cluster_agent()?;

        Ok(CouchbaseAnalyticsClient::new(
            CouchbaseAgentProvider::with_agent(agent.clone()),
        ))
    }

    fn merge_options(
        opts: &mut OnDemandAgentManagerOptions,
        extra_opts: HashMap<String, Vec<String>>,
    ) -> error::Result<()> {
        for (k, v) in extra_opts {
            match k.as_str() {
                "kv_connect_timeout" => {
                    opts.connect_timeout =
                        Some(Duration::from_millis(v[0].parse().map_err(|e| {
                            error::Error {
                                msg: format!("{}", e),
                            }
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
