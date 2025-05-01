use crate::cbconfig;
use crate::cbconfig::TerseConfig;
use crate::configparser::ConfigParser;
use crate::error::{Error, Result};
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::KvClientManager;
use crate::memdx::request::GetClusterConfigRequest;
use crate::parsedconfig::ParsedConfig;
use futures::future::err;
use log::{debug, error, trace};
use std::cmp::Ordering;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::sleep;

pub(crate) trait ConfigWatcher: Send + Sync {
    fn watch(&self, on_shutdown_rx: Receiver<()>) -> Receiver<ParsedConfig>;
    fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()>;
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigWatcherMemdConfig {
    pub endpoints: Vec<String>,
}

pub(crate) struct ConfigWatcherMemdOptions<M: KvClientManager> {
    pub polling_period: Duration,
    pub kv_client_manager: Arc<M>,
}

pub struct ConfigWatcherMemdInner<M: KvClientManager> {
    pub polling_period: Duration,
    pub kv_client_manager: Arc<M>,
    endpoints: Mutex<Vec<String>>,
}

impl<M> ConfigWatcherMemdInner<M>
where
    M: KvClientManager,
{
    pub fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
        let mut endpoints = self.endpoints.lock().unwrap();
        *endpoints = config.endpoints;

        Ok(())
    }

    pub async fn watch(
        &self,
        mut on_shutdown_rx: Receiver<()>,
        on_new_config_tx: Sender<ParsedConfig>,
    ) {
        let mut recent_endpoints = vec![];
        let mut all_endpoints_failed = true;
        let mut last_sent_config = None;

        loop {
            let mut endpoints = vec![];
            {
                // Ensure the mutex isn't held across an await.
                let endpoints_guard = self.endpoints.lock().unwrap();
                for endpoint in endpoints_guard.iter() {
                    endpoints.push(endpoint.clone());
                }
            }

            if endpoints.is_empty() {
                select! {
                    _ = on_shutdown_rx.recv() => {
                        return;
                    },
                    _ = sleep(self.polling_period) => {
                        continue;
                    }
                }
            }

            let mut remaining_endpoints = vec![];
            for endpoint in endpoints {
                if !recent_endpoints.contains(&endpoint) {
                    remaining_endpoints.push(endpoint);
                }
            }

            let endpoint = if remaining_endpoints.is_empty() {
                if all_endpoints_failed {
                    select! {
                        _ = on_shutdown_rx.recv() => {
                            return;
                        },
                        _ = sleep(self.polling_period) => {}
                    }
                }

                recent_endpoints = vec![];
                all_endpoints_failed = true;
                continue;
            } else {
                remaining_endpoints.remove(0)
            };

            recent_endpoints.push(endpoint.clone());

            let parsed_config = match self.poll_one(&endpoint).await {
                Ok(c) => c,
                Err(e) => {
                    // TODO: log
                    dbg!(e);
                    continue;
                }
            };

            all_endpoints_failed = false;

            if let Some(config) = &last_sent_config {
                if let Some(cmp) = parsed_config.partial_cmp(config) {
                    if cmp == Ordering::Greater {
                        // TODO: log.
                        on_new_config_tx
                            .send(parsed_config.clone())
                            .unwrap_or_default();
                        last_sent_config = Some(parsed_config);
                    }
                }
            } else {
                on_new_config_tx
                    .send(parsed_config.clone())
                    .unwrap_or_default();
                last_sent_config = Some(parsed_config);
            }

            select! {
                _ = on_shutdown_rx.recv() => {
                    return;
                },
                _ = sleep(self.polling_period) => {}
            }
        }
    }

    async fn poll_one(&self, endpoint: &str) -> Result<ParsedConfig> {
        debug!("Polling config from {}", &endpoint);

        let client = self.kv_client_manager.get_client(endpoint).await?;

        let hostname = client.remote_hostname();

        let resp = client
            .get_cluster_config(GetClusterConfigRequest {})
            .await
            .map_err(Error::new_contextual_memdx_error)?;

        let config = cbconfig::parse::parse_terse_config(&resp.config, hostname)?;

        trace!("Poller fetched new config {:?}", &config);

        ConfigParser::parse_terse_config(config, hostname)
    }
}

pub(crate) struct ConfigWatcherMemd<M: KvClientManager> {
    inner: Arc<ConfigWatcherMemdInner<M>>,
}

impl<M> ConfigWatcherMemd<M>
where
    M: KvClientManager + 'static,
{
    pub fn new(config: ConfigWatcherMemdConfig, opts: ConfigWatcherMemdOptions<M>) -> Self {
        Self {
            inner: Arc::new(ConfigWatcherMemdInner {
                polling_period: opts.polling_period,
                kv_client_manager: opts.kv_client_manager,
                endpoints: Mutex::new(config.endpoints),
            }),
        }
    }
}

impl<M> ConfigWatcher for ConfigWatcherMemd<M>
where
    M: KvClientManager + 'static,
{
    fn watch(&self, on_shutdown_rx: Receiver<()>) -> Receiver<ParsedConfig> {
        let (on_new_config_tx, on_new_config_rx) = broadcast::channel::<ParsedConfig>(1);

        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.watch(on_shutdown_rx, on_new_config_tx).await;
            debug!("config poll exit")
        });

        on_new_config_rx
    }

    fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
        self.inner.reconfigure(config)
    }
}
