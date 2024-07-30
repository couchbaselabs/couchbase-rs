use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::error::ErrorKind;
use crate::error::Result;
use crate::kvclient::{KvClient, KvClientConfig};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::{KvClientPool, KvClientPoolConfig, KvClientPoolOptions};
use crate::memdx::dispatcher::OrphanResponseHandler;

pub(crate) type KvClientManagerClientType<M> =
    <<M as KvClientManager>::Pool as KvClientPool>::Client;

pub(crate) trait KvClientManager: Sized + Send + Sync {
    type Pool: KvClientPool + Send + Sync;

    fn new(
        config: KvClientManagerConfig,
        opts: KvClientManagerOptions,
    ) -> impl Future<Output = Result<Self>> + Send;
    fn reconfigure(&self, config: KvClientManagerConfig)
        -> impl Future<Output = Result<()>> + Send;
    fn get_client(
        &self,
        endpoint: String,
    ) -> impl Future<Output = Result<Arc<KvClientManagerClientType<Self>>>> + Send;
    fn get_random_client(
        &self,
    ) -> impl Future<Output = Result<Arc<KvClientManagerClientType<Self>>>> + Send;
    fn shutdown_client(
        &self,
        endpoint: String,
        client: Arc<KvClientManagerClientType<Self>>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug)]
pub(crate) struct KvClientManagerConfig {
    pub num_pool_connections: usize,
    pub clients: HashMap<String, KvClientConfig>,
}

#[derive(Clone)]
pub(crate) struct KvClientManagerOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: OrphanResponseHandler,
}

#[derive(Debug)]
struct KvClientManagerPool<P>
where
    P: KvClientPool,
{
    config: KvClientPoolConfig,
    pool: Arc<P>,
}

#[derive(Debug, Default)]
struct KvClientManagerState<P>
where
    P: KvClientPool,
{
    pub client_pools: HashMap<String, KvClientManagerPool<P>>,
}

pub(crate) struct StdKvClientManager<P>
where
    P: KvClientPool,
{
    state: Mutex<KvClientManagerState<P>>,
    opts: KvClientManagerOptions,
}

impl<P> StdKvClientManager<P>
where
    P: KvClientPool,
{
    async fn get_pool(&self, endpoint: String) -> Result<Arc<P>> {
        let state = self.state.lock().await;

        let pool = match state.client_pools.get(&endpoint) {
            Some(p) => p,
            None => {
                return Err(ErrorKind::EndpointNotKnown { endpoint }.into());
            }
        };

        Ok(pool.pool.clone())
    }

    async fn get_random_pool(&self) -> Result<Arc<P>> {
        let state = self.state.lock().await;

        // Just pick one at random for now
        if let Some((_, pool)) = state.client_pools.iter().next() {
            return Ok(pool.pool.clone());
        }

        Err(ErrorKind::NoEndpointsAvailable.into())
    }

    async fn create_pool(&self, pool_config: KvClientPoolConfig) -> KvClientManagerPool<P> {
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
        }
    }
}

impl<P> KvClientManager for StdKvClientManager<P>
where
    P: KvClientPool,
{
    type Pool = P;

    async fn new(config: KvClientManagerConfig, opts: KvClientManagerOptions) -> Result<Self> {
        let manager = Self {
            state: Mutex::new(KvClientManagerState {
                client_pools: Default::default(),
            }),
            opts,
        };

        manager.reconfigure(config).await?;
        Ok(manager)
    }

    async fn reconfigure(&self, config: KvClientManagerConfig) -> Result<()> {
        let mut guard = self.state.lock().await;

        let mut old_pools = std::mem::take(&mut guard.client_pools);

        let mut new_state = KvClientManagerState::<P> {
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

    async fn get_client(&self, endpoint: String) -> Result<Arc<KvClientManagerClientType<Self>>> {
        let pool = self.get_pool(endpoint).await?;

        pool.get_client().await
    }

    async fn get_random_client(&self) -> Result<Arc<KvClientManagerClientType<Self>>> {
        let pool = self.get_random_pool().await?;

        pool.get_client().await
    }

    async fn shutdown_client(
        &self,
        endpoint: String,
        client: Arc<KvClientManagerClientType<Self>>,
    ) -> Result<()> {
        let pool = self.get_pool(endpoint).await?;

        pool.shutdown_client(client).await;

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self.state.lock().await;

        let mut old_pools = std::mem::take(&mut guard.client_pools);

        for (_, pool) in old_pools {
            // TODO: log error.
            pool.pool.close().await.unwrap_or_default();
        }

        Ok(())
    }
}

pub(crate) async fn orchestrate_memd_client<Resp, M, Fut>(
    manager: Arc<M>,
    endpoint: String,
    mut operation: impl FnMut(Arc<KvClientManagerClientType<M>>) -> Fut,
) -> Result<Resp>
where
    M: KvClientManager,
    Fut: Future<Output = Result<Resp>> + Send,
{
    loop {
        let client = manager.get_client(endpoint.clone()).await?;

        let res = operation(client.clone()).await;
        return match res {
            Ok(r) => Ok(r),
            Err(e) => {
                if let Some(memdx_err) = e.is_memdx_error() {
                    if memdx_err.is_dispatch_error() {
                        // This was a dispatch error, so we can just try with
                        // a different client instead...
                        // TODO: Log something
                        manager
                            .shutdown_client(endpoint.clone(), client)
                            .await
                            .unwrap_or_default();
                        continue;
                    }
                }

                Err(e)
            }
        };
    }
}
