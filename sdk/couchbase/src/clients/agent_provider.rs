use couchbase_core::agent::Agent;
use couchbase_core::ondemand_agentmanager::OnDemandAgentManager;
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct CouchbaseAgentProvider {
    inner: Arc<CouchbaseAgentProviderInner>,
    agent_create_handle: Option<Arc<tokio::task::JoinHandle<()>>>,
}

struct CouchbaseAgentProviderInner {
    agent: RwLock<Option<Agent>>,
    waiter: Notify,
}

impl CouchbaseAgentProvider {
    pub fn with_agent(agent: Agent) -> Self {
        Self {
            inner: Arc::new(CouchbaseAgentProviderInner {
                agent: RwLock::new(Some(agent)),
                waiter: Notify::new(),
            }),
            agent_create_handle: None,
        }
    }

    pub fn with_bucket(agent_manager: Arc<OnDemandAgentManager>, bucket_name: String) -> Self {
        let inner = Arc::new(CouchbaseAgentProviderInner {
            agent: RwLock::new(None),
            waiter: Notify::new(),
        });

        let inner_clone = inner.clone();
        let handle = tokio::spawn(async move {
            loop {
                let agent = match agent_manager.get_bucket_agent(bucket_name.clone()).await {
                    Ok(agent) => agent,
                    Err(e) => {
                        log::error!("failed to get agent for bucket {bucket_name}: {e}");
                        continue;
                    }
                };
                {
                    let mut guard = inner_clone.agent.write().unwrap();
                    *guard = Some(agent);
                }
                inner_clone.waiter.notify_waiters();
                return;
            }
        });

        Self {
            inner,
            agent_create_handle: Some(Arc::new(handle)),
        }
    }

    // get_agent will return the agent if it is already available, otherwise it will wait until it is available.
    pub async fn get_agent(&self) -> Agent {
        {
            let guard = self.inner.agent.read().unwrap();
            if let Some(agent) = guard.as_ref() {
                return agent.clone();
            }
        }

        self.inner.waiter.notified().await;
        Box::pin(self.get_agent()).await
    }
}
