use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;

use crate::error::ErrorKind;
use crate::error::Result;
use crate::kvclient::{KvClient, KvClientConfig};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::{KvClientPool, KvClientPoolConfig, KvClientPoolOptions};
use crate::memdx::dispatcher::OrphanResponseHandler;
use crate::tracingcomponent::TracingComponent;

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
        endpoint: &str,
    ) -> impl Future<Output = Result<Arc<KvClientManagerClientType<Self>>>> + Send;
    fn get_random_client(
        &self,
    ) -> impl Future<Output = Result<Arc<KvClientManagerClientType<Self>>>> + Send;
    fn shutdown_client(
        &self,
        endpoint: Option<&str>,
        client: Arc<KvClientManagerClientType<Self>>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct KvClientManagerConfig {
    pub num_pool_connections: usize,
    pub clients: HashMap<String, KvClientConfig>,
}

#[derive(Clone)]
pub(crate) struct KvClientManagerOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: OrphanResponseHandler,
    pub disable_decompression: bool,
    pub tracing: Arc<TracingComponent>,
}

#[derive(Debug, Default, Clone)]
struct KvClientManagerState<P>
where
    P: KvClientPool,
{
    pub client_pools: HashMap<String, Arc<P>>,
}

pub(crate) struct StdKvClientManager<P>
where
    P: KvClientPool,
{
    closed: Mutex<bool>,
    state: ArcSwap<KvClientManagerState<P>>,
    opts: KvClientManagerOptions,
}

impl<P> StdKvClientManager<P>
where
    P: KvClientPool,
{
    async fn get_pool(&self, endpoint: &str) -> Result<Arc<P>> {
        let state = self.state.load();

        let pool = match state.client_pools.get(endpoint) {
            Some(p) => p,
            None => {
                return Err(ErrorKind::EndpointNotKnown {
                    endpoint: endpoint.to_string(),
                }
                .into());
            }
        };

        Ok(pool.clone())
    }

    async fn get_random_pool(&self) -> Result<Arc<P>> {
        let state = self.state.load();

        // Just pick one at random for now
        if let Some((_, pool)) = state.client_pools.iter().next() {
            return Ok(pool.clone());
        }

        Err(ErrorKind::NoEndpointsAvailable.into())
    }

    async fn create_pool(&self, pool_config: KvClientPoolConfig) -> Arc<P> {
        let pool = P::new(
            pool_config.clone(),
            KvClientPoolOptions {
                connect_timeout: self.opts.connect_timeout,
                connect_throttle_period: self.opts.connect_throttle_period,
                orphan_handler: self.opts.orphan_handler.clone(),
                disable_decompression: self.opts.disable_decompression,
                tracing: self.opts.tracing.clone(),
            },
        )
        .await;

        Arc::new(pool)
    }

    async fn shutdown_random_client(
        &self,
        client: Arc<KvClientManagerClientType<Self>>,
    ) -> Result<()> {
        let state = self.state.load();

        for (_, pool) in state.client_pools.iter() {
            pool.shutdown_client(client.clone()).await;
        }

        Ok(())
    }
}

impl<P> KvClientManager for StdKvClientManager<P>
where
    P: KvClientPool,
{
    type Pool = P;

    async fn new(config: KvClientManagerConfig, opts: KvClientManagerOptions) -> Result<Self> {
        let manager = Self {
            closed: Mutex::new(false),
            state: ArcSwap::from_pointee(KvClientManagerState {
                client_pools: Default::default(),
            }),
            opts,
        };

        manager.reconfigure(config).await?;
        Ok(manager)
    }

    async fn reconfigure(&self, config: KvClientManagerConfig) -> Result<()> {
        let mut guard = self.closed.lock().await;
        if *guard {
            return Err(ErrorKind::IllegalState {
                msg: "reconfigure called after close".to_string(),
            }
            .into());
        }

        let mut new_state = KvClientManagerState::<P> {
            client_pools: Default::default(),
        };

        let state = self.state.load();

        let mut old_pools = HashMap::new();
        old_pools.clone_from(&state.client_pools);

        for (endpoint, endpoint_config) in config.clients {
            let pool_config = KvClientPoolConfig {
                num_connections: config.num_pool_connections,
                client_config: endpoint_config,
            };

            let old_pool = old_pools.remove(&endpoint);
            let new_pool = if let Some(pool) = old_pool {
                // TODO: log on error.
                if pool.reconfigure(pool_config.clone()).await.is_ok() {
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
            pool.close().await.unwrap_or_default();
        }

        self.state.store(Arc::from(new_state));

        Ok(())
    }

    async fn get_client(&self, endpoint: &str) -> Result<Arc<KvClientManagerClientType<Self>>> {
        let pool = self.get_pool(endpoint).await?;

        pool.get_client().await
    }

    async fn get_random_client(&self) -> Result<Arc<KvClientManagerClientType<Self>>> {
        let pool = self.get_random_pool().await?;

        pool.get_client().await
    }

    async fn shutdown_client(
        &self,
        endpoint: Option<&str>,
        client: Arc<KvClientManagerClientType<Self>>,
    ) -> Result<()> {
        if let Some(ep) = endpoint {
            let pool = self.get_pool(ep).await?;

            pool.shutdown_client(client).await;

            return Ok(());
        }

        // we don't know which endpoint this belongs to, so we need to send the
        // shutdown request to all the possibilities...
        self.shutdown_random_client(client).await
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self.closed.lock().await;
        if *guard {
            return Err(ErrorKind::IllegalState {
                msg: "close called twice".to_string(),
            }
            .into());
        }
        *guard = true;

        let state = self.state.load();

        let mut old_pools = HashMap::new();
        old_pools.clone_from(&state.client_pools);

        self.state.swap(Arc::new(KvClientManagerState {
            client_pools: HashMap::new(),
        }));

        for (_, pool) in old_pools {
            // TODO: log error.
            pool.close().await.unwrap_or_default();
        }

        Ok(())
    }
}

pub(crate) async fn orchestrate_memd_client<Resp, M, Fut>(
    manager: Arc<M>,
    endpoint: &str,
    mut operation: impl FnMut(Arc<KvClientManagerClientType<M>>) -> Fut,
) -> Result<Resp>
where
    M: KvClientManager,
    Fut: Future<Output = Result<Resp>> + Send,
{
    loop {
        let client = manager.get_client(endpoint).await?;

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
                            .shutdown_client(Some(endpoint), client)
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

pub(crate) async fn orchestrate_random_memd_client<Resp, M, Fut>(
    manager: Arc<M>,
    mut operation: impl FnMut(Arc<KvClientManagerClientType<M>>) -> Fut,
) -> Result<Resp>
where
    M: KvClientManager,
    Fut: Future<Output = Result<Resp>> + Send,
{
    loop {
        let client = manager.get_random_client().await?;

        let res = operation(client.clone()).await;
        let res = match res {
            Ok(r) => Ok(r),
            Err(e) => {
                if let Some(memdx_err) = e.is_memdx_error() {
                    if memdx_err.is_dispatch_error() {
                        // This was a dispatch error, so we can just try with
                        // a different client instead...
                        // TODO: Log something
                        manager
                            .shutdown_client(None, client)
                            .await
                            .unwrap_or_default();
                        continue;
                    }
                }

                Err(e)
            }
        };
        return res;
    }
}
