use std::cmp::Ordering;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::sleep;

use crate::cbconfig::TerseConfig;
use crate::configparser::ConfigParser;
use crate::error::Result;
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::KvClientManager;
use crate::memdx::request::GetClusterConfigRequest;
use crate::parsedconfig::ParsedConfig;

pub(crate) trait ConfigWatcher {
    fn watch(&self, on_shutdown_rx: Receiver<()>) -> Receiver<ParsedConfig>;
    fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> impl Future<Output = Result<()>>;
}

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
    pub async fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
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
                    _ = sleep(self.polling_period) => {}
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

            let parsed_config = match self.poll_one(endpoint).await {
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

    async fn poll_one(&self, endpoint: String) -> Result<ParsedConfig> {
        let client = self.kv_client_manager.get_client(endpoint).await?;

        let addr = client.remote_addr();
        let host = addr.ip();

        let resp = client
            .get_cluster_config(GetClusterConfigRequest {})
            .await?;

        let config: TerseConfig = serde_json::from_slice(resp.config.as_slice())?;

        ConfigParser::parse_terse_config(config, &host.to_string())
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
        });

        on_new_config_rx
    }

    async fn reconfigure(&self, config: ConfigWatcherMemdConfig) -> Result<()> {
        self.inner.reconfigure(config).await
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::broadcast;
    use tokio::time::sleep;

    use crate::authenticator::PasswordAuthenticator;
    use crate::configwatcher::{
        ConfigWatcher, ConfigWatcherMemd, ConfigWatcherMemdConfig, ConfigWatcherMemdOptions,
    };
    use crate::kvclient::{KvClientConfig, StdKvClient};
    use crate::kvclientmanager::{
        KvClientManager, KvClientManagerConfig, KvClientManagerOptions, StdKvClientManager,
    };
    use crate::kvclientpool::NaiveKvClientPool;
    use crate::memdx::client::Client;

    #[tokio::test]
    async fn fetches_configs() {
        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(
                PasswordAuthenticator {
                    username: "Administrator".to_string(),
                    password: "password".to_string(),
                }
                .into(),
            )),
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
                    orphan_handler: Arc::new(|_| {}),
                },
            )
            .await
            .unwrap();

        let config = ConfigWatcherMemdConfig {
            endpoints: vec!["192.168.107.128:11210".to_string()],
        };
        let opts = ConfigWatcherMemdOptions {
            polling_period: Duration::from_secs(1),
            kv_client_manager: Arc::new(manager),
        };
        let watcher = ConfigWatcherMemd::new(config, opts);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let mut receiver = watcher.watch(shutdown_rx);

        tokio::spawn(async move {
            sleep(Duration::from_secs(5)).await;
            shutdown_tx.send(()).unwrap();
        });

        loop {
            let config = match receiver.recv().await {
                Ok(c) => c,
                Err(e) => {
                    dbg!(e);
                    return;
                }
            };
            dbg!(config);
        }
    }
}
