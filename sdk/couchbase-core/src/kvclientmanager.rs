use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

use crate::error::CoreError;
use crate::kvclient::{KvClient, KvClientConfig};
use crate::kvclientpool::{KvClientPool, KvClientPoolConfig, KvClientPoolOptions};
use crate::memdx::packet::ResponsePacket;
use crate::result::CoreResult;

pub(crate) trait KvClientManager<P, K>: Sized + Send + Sync {
    fn new(
        config: KvClientManagerConfig,
        opts: KvClientManagerOptions,
    ) -> impl Future<Output = CoreResult<Self>> + Send;
    fn reconfigure(
        &self,
        config: KvClientManagerConfig,
    ) -> impl Future<Output = CoreResult<()>> + Send;
    fn get_client(&self, endpoint: String) -> impl Future<Output = CoreResult<Arc<K>>> + Send;
    fn get_random_client(&self) -> impl Future<Output = CoreResult<Arc<K>>> + Send;
    fn shutdown_client(
        &self,
        endpoint: String,
        client: Arc<K>,
    ) -> impl Future<Output = CoreResult<()>> + Send;
    fn close(&self) -> impl Future<Output = CoreResult<()>> + Send;
    fn orchestrate_operation<R, Fut>(
        &self,
        endpoint: String,
        operation: impl Fn(Arc<K>) -> Fut,
    ) -> impl Future<Output = CoreResult<R>>
    where
        Fut: Future<Output = CoreResult<R>> + Send;
}

#[derive(Debug)]
pub(crate) struct KvClientManagerConfig {
    pub num_pool_connections: usize,
    pub clients: HashMap<String, KvClientConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct KvClientManagerOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: Arc<UnboundedSender<ResponsePacket>>,
}

#[derive(Debug)]
struct KvClientManagerPool<P, K>
where
    P: KvClientPool<K>,
{
    config: KvClientPoolConfig,
    pool: Arc<P>,
    _phantom_client_type: PhantomData<K>,
}

#[derive(Debug, Default)]
struct KvClientManagerState<P, K>
where
    P: KvClientPool<K>,
{
    pub client_pools: HashMap<String, KvClientManagerPool<P, K>>,
}

pub(crate) struct StdKvClientManager<P, K>
where
    P: KvClientPool<K>,
{
    state: Mutex<KvClientManagerState<P, K>>,
    opts: KvClientManagerOptions,
}

impl<P, K> StdKvClientManager<P, K>
where
    K: KvClient,
    P: KvClientPool<K>,
{
    async fn get_pool(&self, endpoint: String) -> CoreResult<Arc<P>> {
        let state = self.state.lock().await;

        let pool = match state.client_pools.get(&endpoint) {
            Some(p) => p,
            None => {
                return Err(CoreError::Placeholder("Endpoint not known".to_string()));
            }
        };

        Ok(pool.pool.clone())
    }

    async fn get_random_pool(&self) -> CoreResult<Arc<P>> {
        let state = self.state.lock().await;

        // Just pick one at random for now
        if let Some((_, pool)) = state.client_pools.iter().next() {
            return Ok(pool.pool.clone());
        }

        Err(CoreError::Placeholder("Endpoint not known".to_string()))
    }

    async fn create_pool(&self, pool_config: KvClientPoolConfig) -> KvClientManagerPool<P, K> {
        let pool = P::new(
            pool_config.clone(),
            KvClientPoolOptions {
                connect_timeout: self.opts.connect_timeout,
                connect_throttle_period: self.opts.connect_throttle_period,
                orphan_handler: self.opts.orphan_handler.clone(),
            },
        )
        .await;

        KvClientManagerPool {
            config: pool_config,
            pool: Arc::new(pool),
            _phantom_client_type: Default::default(),
        }
    }
}

impl<P, K> KvClientManager<P, K> for StdKvClientManager<P, K>
where
    K: KvClient,
    P: KvClientPool<K>,
{
    async fn new(config: KvClientManagerConfig, opts: KvClientManagerOptions) -> CoreResult<Self> {
        let manager = Self {
            state: Mutex::new(KvClientManagerState {
                client_pools: Default::default(),
            }),
            opts,
        };

        manager.reconfigure(config).await?;
        Ok(manager)
    }

    async fn reconfigure(&self, config: KvClientManagerConfig) -> CoreResult<()> {
        let mut guard = self.state.lock().await;

        let mut old_pools = std::mem::take(&mut guard.client_pools);

        let mut new_state = KvClientManagerState::<P, K> {
            client_pools: Default::default(),
        };

        for (endpoint, endpoint_config) in config.clients {
            let pool_config = KvClientPoolConfig {
                num_connections: config.num_pool_connections,
                client_config: endpoint_config,
            };

            let old_pool = old_pools.remove(&endpoint);
            let new_pool = if let Some(pool) = old_pool {
                // TODO: log on error.
                if pool.pool.reconfigure(pool_config.clone()).await.is_ok() {
                    pool
                } else {
                    self.create_pool(pool_config).await
                }
            } else {
                self.create_pool(pool_config).await
            };

            new_state.client_pools.insert(endpoint, new_pool);
        }

        for (_, pool) in old_pools {
            // TODO: log?
            pool.pool.close().await.unwrap_or_default();
        }

        *guard = new_state;

        Ok(())
    }

    async fn get_client(&self, endpoint: String) -> CoreResult<Arc<K>> {
        let pool = self.get_pool(endpoint).await?;

        pool.get_client().await
    }

    async fn get_random_client(&self) -> CoreResult<Arc<K>> {
        let pool = self.get_random_pool().await?;

        pool.get_client().await
    }

    async fn shutdown_client(&self, endpoint: String, client: Arc<K>) -> CoreResult<()> {
        let pool = self.get_pool(endpoint).await?;

        pool.shutdown_client(client).await;

        Ok(())
    }

    async fn close(&self) -> CoreResult<()> {
        let mut guard = self.state.lock().await;

        let mut old_pools = std::mem::take(&mut guard.client_pools);

        for (_, pool) in old_pools {
            // TODO: log error.
            pool.pool.close().await.unwrap_or_default();
        }

        Ok(())
    }

    async fn orchestrate_operation<R, Fut>(
        &self,
        endpoint: String,
        operation: impl Fn(Arc<K>) -> Fut,
    ) -> CoreResult<R>
    where
        Fut: Future<Output = CoreResult<R>> + Send,
    {
        loop {
            let client = self.get_client(endpoint.clone()).await?;

            let res = operation(client.clone()).await;
            match res {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => match e {
                    CoreError::Dispatch(_) => {
                        // This was a dispatch error, so we can just try with
                        // a different client instead...
                        // TODO: Log something
                        self.shutdown_client(endpoint.clone(), client)
                            .await
                            .unwrap_or_default();
                    }
                    _ => {
                        return Err(e);
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::kvclient::{KvClient, KvClientConfig, StdKvClient};
    use crate::kvclientmanager::{
        KvClientManager, KvClientManagerConfig, KvClientManagerOptions, StdKvClientManager,
    };
    use crate::kvclientpool::{KvClientPool, NaiveKvClientPool};
    use crate::memdx::client::Client;
    use crate::memdx::packet::ResponsePacket;
    use crate::memdx::request::{GetRequest, SetRequest};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roundtrip_a_request() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let (orphan_tx, mut orphan_rx) = unbounded_channel::<ResponsePacket>();

        tokio::spawn(async move {
            loop {
                match orphan_rx.recv().await {
                    Some(resp) => {
                        dbg!("unexpected orphan", resp);
                    }
                    None => {
                        return;
                    }
                }
            }
        });

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(PasswordAuthenticator {
                username: "Administrator".to_string(),
                password: "password".to_string(),
            })),
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

        let manager: StdKvClientManager<
            NaiveKvClientPool<StdKvClient<Client>>,
            StdKvClient<Client>,
        > = StdKvClientManager::new(
            manger_config,
            KvClientManagerOptions {
                connect_timeout: Default::default(),
                connect_throttle_period: Default::default(),
                orphan_handler: Arc::new(orphan_tx),
            },
        )
        .await
        .unwrap();

        let result = manager
            .orchestrate_operation(
                "192.168.107.128:11210".to_string(),
                |client: Arc<StdKvClient<Client>>| async move {
                    client
                        .set(SetRequest {
                            collection_id: 0,
                            key: "test".as_bytes().into(),
                            vbucket_id: 1,
                            flags: 0,
                            value: "test".as_bytes().into(),
                            datatype: 0,
                            expiry: None,
                            preserve_expiry: None,
                            cas: None,
                            on_behalf_of: None,
                            durability_level: None,
                            durability_level_timeout: None,
                        })
                        .await
                },
            )
            .await
            .unwrap();

        dbg!(result);

        let client = manager
            .get_client("192.168.107.128:11210".to_string())
            .await
            .unwrap();

        let result = client
            .set(SetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                flags: 0,
                value: "test".as_bytes().into(),
                datatype: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                on_behalf_of: None,
                durability_level: None,
                durability_level_timeout: None,
            })
            .await
            .unwrap();

        dbg!(result);

        let get_result = client
            .get(GetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                on_behalf_of: None,
            })
            .await
            .unwrap();

        dbg!(get_result);

        manager.close().await.unwrap();
    }
}
