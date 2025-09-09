use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::executor::block_on;
use log::debug;
use tokio::sync::{Mutex, Notify};

use crate::agent::Agent;
use crate::auth_mechanism::AuthMechanism;
use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::options::agent::{
    AgentOptions, CompressionConfig, ConfigPollerConfig, HttpConfig, KvConfig, SeedConfig,
};
use crate::options::ondemand_agentmanager::OnDemandAgentManagerOptions;
use crate::tls_config::TlsConfig;

pub struct OnDemandAgentManager {
    opts: OnDemandAgentManagerOptions,
    // This is Arc so that we can provide a consistent API for handing out agents.
    cluster_agent: Arc<Agent>,
    fast_map: ArcSwap<HashMap<String, Weak<Agent>>>,
    slow_map: Mutex<HashMap<String, Arc<Agent>>>,
    notif_map: Mutex<HashMap<String, Arc<Notify>>>,
}

impl OnDemandAgentManager {
    pub async fn new(opts: OnDemandAgentManagerOptions) -> error::Result<Self> {
        let cluster_agent = Arc::new(Agent::new(opts.clone().into()).await?);

        Ok(Self {
            opts,
            cluster_agent,
            fast_map: Default::default(),
            slow_map: Default::default(),
            notif_map: Default::default(),
        })
    }

    pub fn get_cluster_agent(&self) -> Weak<Agent> {
        Arc::downgrade(&self.cluster_agent)
    }

    pub async fn get_bucket_agent(
        &self,
        bucket_name: impl Into<String>,
    ) -> error::Result<Weak<Agent>> {
        let bucket_name = bucket_name.into();
        loop {
            let fast_map = self.fast_map.load();
            if let Some(agent) = fast_map.get(&bucket_name) {
                return Ok(agent.clone());
            }

            self.load_bucket_agent_slow(&bucket_name).await?;

            let slow_map = self.slow_map.lock().await;
            let mut fast_map = HashMap::with_capacity(slow_map.len());
            for (name, agent) in slow_map.iter() {
                fast_map.insert(name.clone(), Arc::downgrade(agent));
            }

            self.fast_map.store(Arc::new(fast_map));
        }
    }

    async fn load_bucket_agent_slow(&self, bucket_name: impl Into<String>) -> error::Result<()> {
        let bucket_name = bucket_name.into();
        let notif = {
            let mut slow_map = self.slow_map.lock().await;
            if slow_map.contains_key(&bucket_name) {
                return Ok(());
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
                return Ok(());
            };

            debug!("Bucket name {} not in any map, creating new", &bucket_name);
            let notif = Arc::new(Notify::new());
            notif_map.insert(bucket_name.clone(), notif.clone());

            notif
        };

        let mut opts: AgentOptions = self.opts.clone().into();
        opts.bucket_name = Some(bucket_name.clone());

        let agent = Arc::new(Agent::new(opts).await?);

        let mut slow_map = self.slow_map.lock().await;
        slow_map.insert(bucket_name.clone(), agent);

        {
            // We remove the entry from the notif map, whilst still under the slow_map lock.
            let mut notif_map = self.notif_map.lock().await;
            notif_map.remove(&bucket_name);
        }

        notif.notify_waiters();

        Ok(())
    }
}
