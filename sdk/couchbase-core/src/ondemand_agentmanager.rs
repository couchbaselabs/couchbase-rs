use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use log::debug;
use tokio::sync::{Mutex, Notify};

use crate::agent::Agent;
use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::options::agent::{AgentOptions, CompressionConfig, ConfigPollerConfig, SeedConfig};
use crate::tls_config::TlsConfig;

#[derive(Clone)]
#[non_exhaustive]
pub struct OnDemandAgentManagerOptions {
    pub seed_config: SeedConfig,
    pub authenticator: Authenticator,

    pub auth_mechanisms: Vec<AuthMechanism>,
    pub tls_config: Option<TlsConfig>,

    pub connect_timeout: Option<Duration>,
    pub connect_throttle_timeout: Option<Duration>,

    pub compression_config: CompressionConfig,
    pub config_poller_config: ConfigPollerConfig,
}

impl OnDemandAgentManagerOptions {
    pub fn new(seed_config: SeedConfig, authenticator: Authenticator) -> Self {
        Self {
            tls_config: None,
            authenticator,
            connect_timeout: None,
            connect_throttle_timeout: None,
            seed_config,
            compression_config: CompressionConfig::default(),
            config_poller_config: ConfigPollerConfig::default(),
            auth_mechanisms: vec![],
        }
    }

    pub fn seed_config(mut self, seed_config: SeedConfig) -> Self {
        self.seed_config = seed_config;
        self
    }

    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = authenticator;
        self
    }

    pub fn tls_config(mut self, tls_config: impl Into<Option<TlsConfig>>) -> Self {
        self.tls_config = tls_config.into();
        self
    }

    pub fn connect_timeout(mut self, connect_timeout: impl Into<Option<Duration>>) -> Self {
        self.connect_timeout = connect_timeout.into();
        self
    }

    pub fn connect_throttle_timeout(
        mut self,
        connect_throttle_timeout: impl Into<Option<Duration>>,
    ) -> Self {
        self.connect_throttle_timeout = connect_throttle_timeout.into();
        self
    }

    pub fn compression_config(mut self, compression_config: CompressionConfig) -> Self {
        self.compression_config = compression_config;
        self
    }

    pub fn config_poller_config(mut self, config_poller_config: ConfigPollerConfig) -> Self {
        self.config_poller_config = config_poller_config;
        self
    }

    pub fn auth_mechanisms(mut self, auth_mechanisms: impl Into<Vec<AuthMechanism>>) -> Self {
        self.auth_mechanisms = auth_mechanisms.into();
        self
    }
}

impl From<OnDemandAgentManagerOptions> for AgentOptions {
    fn from(opts: OnDemandAgentManagerOptions) -> Self {
        AgentOptions {
            tls_config: opts.tls_config,
            authenticator: opts.authenticator,
            bucket_name: None,
            connect_timeout: opts.connect_timeout,
            connect_throttle_timeout: opts.connect_throttle_timeout,
            seed_config: opts.seed_config,
            compression_config: opts.compression_config,
            config_poller_config: opts.config_poller_config,
            auth_mechanisms: opts.auth_mechanisms,
        }
    }
}

impl From<AgentOptions> for OnDemandAgentManagerOptions {
    fn from(opts: AgentOptions) -> Self {
        OnDemandAgentManagerOptions {
            authenticator: opts.authenticator,
            tls_config: opts.tls_config,
            connect_timeout: opts.connect_timeout,
            connect_throttle_timeout: opts.connect_throttle_timeout,
            seed_config: opts.seed_config,
            compression_config: opts.compression_config,
            config_poller_config: opts.config_poller_config,
            auth_mechanisms: opts.auth_mechanisms,
        }
    }
}

pub struct OnDemandAgentManager {
    opts: OnDemandAgentManagerOptions,
    cluster_agent: Agent,
    fast_map: ArcSwap<HashMap<String, Agent>>,
    slow_map: Mutex<HashMap<String, Agent>>,
    notif_map: Mutex<HashMap<String, Arc<Notify>>>,
}

impl OnDemandAgentManager {
    pub async fn new(opts: OnDemandAgentManagerOptions) -> error::Result<Self> {
        let cluster_agent = Agent::new(opts.clone().into()).await?;

        Ok(Self {
            opts,
            cluster_agent,
            fast_map: Default::default(),
            slow_map: Default::default(),
            notif_map: Default::default(),
        })
    }

    pub fn get_cluster_agent(&self) -> &Agent {
        &self.cluster_agent
    }

    pub async fn get_bucket_agent(&self, bucket_name: impl Into<String>) -> error::Result<Agent> {
        let bucket_name = bucket_name.into();
        let fast_map = self.fast_map.load();
        if let Some(agent) = fast_map.get(&bucket_name) {
            return Ok(agent.clone());
        }

        self.get_bucket_agent_slow(bucket_name).await
    }

    async fn get_bucket_agent_slow(&self, bucket_name: impl Into<String>) -> error::Result<Agent> {
        let bucket_name = bucket_name.into();
        let notif = {
            let mut slow_map = self.slow_map.lock().await;
            if let Some(agent) = slow_map.get(&bucket_name) {
                return Ok(agent.clone());
            }

            debug!(
                "Bucket name {} not in slow map, checking notif map",
                &bucket_name
            );
            // If we don't have an agent then check the notif map to see if someone else is getting
            // an agent already. Note that we're still inside the slow_map lock here.
            let mut notif_map = self.notif_map.lock().await;
            if let Some(notif) = notif_map.get(&bucket_name) {
                let notif = notif.clone();
                drop(slow_map);
                drop(notif_map);

                debug!("Bucket name {} in notif map, awaiting update", &bucket_name);
                notif.notified().await;
                debug!("Bucket name {} received updated", &bucket_name);
                return Box::pin(self.get_bucket_agent_slow(bucket_name)).await;
            };

            debug!("Bucket name {} not in any map, creating new", &bucket_name);
            let notif = Arc::new(Notify::new());
            notif_map.insert(bucket_name.clone(), notif.clone());

            notif
        };

        let mut opts: AgentOptions = self.opts.clone().into();
        opts.bucket_name = Some(bucket_name.clone());

        let agent = Agent::new(opts).await?;

        let mut slow_map = self.slow_map.lock().await;
        slow_map.insert(bucket_name.clone(), agent.clone());

        {
            // We remove the entry from the notif map, whilst still under the slow_map lock.
            let mut notif_map = self.notif_map.lock().await;
            notif_map.remove(&bucket_name);
        }

        let mut fast_map = HashMap::with_capacity(slow_map.len());
        for (name, agent) in slow_map.iter() {
            fast_map.insert(name.clone(), agent.clone());
        }

        self.fast_map.store(Arc::new(fast_map));

        notif.notify_waiters();

        Ok(agent)
    }
}
