use std::future::Future;
use std::ops::{Deref, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep};

use crate::error::CoreError;
use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions};
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::packet::ResponsePacket;
use crate::result::CoreResult;

pub(crate) trait KvClientPool: Sized + Send + Sync {
    type Client: KvClient + Send + Sync;

    fn new(
        config: KvClientPoolConfig,
        opts: KvClientPoolOptions,
    ) -> impl Future<Output = Self> + Send;
    fn get_client(&self) -> impl Future<Output = CoreResult<Arc<Self::Client>>> + Send;
    fn shutdown_client(&self, client: Arc<Self::Client>) -> impl Future<Output = ()> + Send;
    fn close(&self) -> impl Future<Output = CoreResult<()>> + Send;
    fn reconfigure(
        &self,
        config: KvClientPoolConfig,
    ) -> impl Future<Output = CoreResult<()>> + Send;
}

#[derive(Debug, Clone)]
pub(crate) struct KvClientPoolConfig {
    pub num_connections: usize,
    pub client_config: KvClientConfig,
}

pub(crate) struct KvClientPoolOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: Arc<UnboundedSender<ResponsePacket>>,
}

struct NaiveKvClientPoolInner<K>
where
    K: KvClient,
{
    connect_timeout: Duration,
    connect_throttle_period: Duration,

    config: KvClientPoolConfig,

    clients: Vec<Arc<K>>,

    client_idx: usize,

    connect_error: Option<CoreError>,
    connect_error_time: Option<Instant>,

    orphan_handler: Arc<UnboundedSender<ResponsePacket>>,

    on_client_close_tx: UnboundedSender<String>,

    closed: AtomicBool,
}

pub(crate) struct NaiveKvClientPool<K: KvClient> {
    inner: Arc<Mutex<NaiveKvClientPoolInner<K>>>,
}

impl<K> NaiveKvClientPoolInner<K>
where
    K: KvClient + PartialEq + Sync + Send + 'static,
{
    pub async fn new(
        config: KvClientPoolConfig,
        opts: KvClientPoolOptions,
        on_client_close_tx: UnboundedSender<String>,
    ) -> Self {
        // TODO: is unbounded the right option?
        let mut inner = NaiveKvClientPoolInner::<K> {
            connect_timeout: opts.connect_timeout,
            connect_throttle_period: opts.connect_throttle_period,
            config,
            closed: AtomicBool::new(false),
            on_client_close_tx,
            orphan_handler: opts.orphan_handler,

            clients: vec![],
            connect_error: None,
            connect_error_time: None,
            client_idx: 0,
        };

        inner.check_connections().await;

        inner
    }

    async fn check_connections(&mut self) {
        let num_wanted_clients = self.config.num_connections;
        let num_active_clients = self.clients.len();

        if num_active_clients > num_wanted_clients {
            let mut num_excess_clients = num_active_clients - num_wanted_clients;
            let mut num_closed_clients = 0;

            while num_excess_clients > 0 {
                let client_to_close = self.clients.remove(0);
                self.shutdown_client(client_to_close).await;

                num_excess_clients -= 1;
                num_closed_clients += 1;
            }
        }

        if num_wanted_clients > num_active_clients {
            let mut num_needed_clients = num_wanted_clients - num_active_clients;
            while num_needed_clients > 0 {
                match self.start_new_client().await {
                    Ok(client) => {
                        self.connect_error = None;
                        self.connect_error_time = None;
                        self.clients.push(Arc::new(client));
                        num_needed_clients -= 1;
                    }
                    Err(e) => {
                        self.connect_error_time = Some(Instant::now());
                        self.connect_error = Some(e);
                    }
                }
            }
        }
    }

    async fn start_new_client(&mut self) -> CoreResult<K> {
        loop {
            if let Some(error_time) = self.connect_error_time {
                let connect_wait_period =
                    self.connect_throttle_period - Instant::now().sub(error_time);

                if !connect_wait_period.is_zero() {
                    sleep(connect_wait_period).await;
                    continue;
                }
            }
            break;
        }

        let mut client_result = K::new(
            self.config.client_config.clone(),
            KvClientOptions {
                orphan_handler: self.orphan_handler.clone(),
                on_close_tx: Some(self.on_client_close_tx.clone()),
            },
        )
        .await;

        if self.closed.load(Ordering::SeqCst) {
            if let Ok(mut client) = client_result {
                // TODO: Log something?
                client.close().await.unwrap_or_default();
            }

            return Err(CoreError::Placeholder("Closed".to_string()));
        }

        client_result
    }

    async fn get_client_slow(&mut self) -> CoreResult<Arc<K>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CoreError::Placeholder("Closed".to_string()));
        }

        if !self.clients.is_empty() {
            let idx = self.client_idx;
            self.client_idx += 1;
            // TODO: is this unwrap ok? It should be...
            let client = self.clients.get(idx % self.clients.len()).unwrap();
            return Ok(client.clone());
        }

        if let Some(e) = &self.connect_error {
            return Err(CoreError::Placeholder(e.to_string()));
        }

        self.check_connections().await;
        Box::pin(self.get_client_slow()).await
    }

    pub async fn get_client(&mut self) -> CoreResult<Arc<K>> {
        if !self.clients.is_empty() {
            let idx = self.client_idx;
            self.client_idx += 1;
            // TODO: is this unwrap ok? It should be...
            let client = self.clients.get(idx % self.clients.len()).unwrap();
            return Ok(client.clone());
        }

        self.get_client_slow().await
    }

    pub async fn shutdown_client(&mut self, client: Arc<K>) {
        let idx = self.clients.iter().position(|x| *x == client);
        if let Some(idx) = idx {
            self.clients.remove(idx);
        }

        // TODO: Should log
        client.close().await.unwrap_or_default();
    }

    pub async fn handle_client_close(&mut self, client_id: String) {
        // TODO: not sure the ordering of close leading to here is great.
        if self.closed.load(Ordering::SeqCst) {
            return;
        }

        let idx = self.clients.iter().position(|x| x.id() == client_id);
        if let Some(idx) = idx {
            self.clients.remove(idx);
        }

        self.check_connections().await;
    }

    pub async fn close(&mut self) -> CoreResult<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(CoreError::Placeholder("Closed".to_string()));
        }

        for mut client in &self.clients {
            // TODO: probably log
            client.close().await.unwrap_or_default();
        }

        Ok(())
    }

    pub async fn reconfigure(&mut self, config: KvClientPoolConfig) -> CoreResult<()> {
        let mut old_clients = self.clients.clone();
        let mut new_clients = vec![];
        for client in old_clients {
            if let Err(e) = client.reconfigure(config.client_config.clone()).await {
                // TODO: log here.
                dbg!(e);
                client.close().await.unwrap_or_default();
                continue;
            };

            new_clients.push(client.clone());
        }
        self.clients = new_clients;
        self.config = config;

        self.check_connections().await;

        Ok(())
    }
}

impl<K> KvClientPool for NaiveKvClientPool<K>
where
    K: KvClient + PartialEq + Sync + Send + 'static,
{
    type Client = K;

    async fn new(config: KvClientPoolConfig, opts: KvClientPoolOptions) -> Self {
        // TODO: is unbounded the right option?
        let (on_client_close_tx, mut on_client_close_rx) = mpsc::unbounded_channel();

        let clients = Arc::new(Mutex::new(
            NaiveKvClientPoolInner::<K>::new(config, opts, on_client_close_tx).await,
        ));

        let reader_clients = clients.clone();
        tokio::spawn(async move {
            loop {
                if let Some(id) = on_client_close_rx.recv().await {
                    reader_clients.lock().await.handle_client_close(id).await;
                } else {
                    return;
                }
            }
        });

        NaiveKvClientPool { inner: clients }
    }

    async fn get_client(&self) -> CoreResult<Arc<Self::Client>> {
        let mut clients = self.inner.lock().await;

        clients.get_client().await
    }

    async fn shutdown_client(&self, client: Arc<Self::Client>) {
        let mut clients = self.inner.lock().await;

        clients.shutdown_client(client).await;
    }

    async fn close(&self) -> CoreResult<()> {
        let mut inner = self.inner.lock().await;
        inner.close().await
    }

    async fn reconfigure(&self, config: KvClientPoolConfig) -> CoreResult<()> {
        let mut inner = self.inner.lock().await;
        inner.reconfigure(config).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::kvclient::{KvClient, KvClientConfig, StdKvClient};
    use crate::kvclientpool::{
        KvClientPool, KvClientPoolConfig, KvClientPoolOptions, NaiveKvClientPool,
    };
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

        let pool_config = KvClientPoolConfig {
            num_connections: 1,
            client_config,
        };

        let pool: NaiveKvClientPool<StdKvClient<Client>> = NaiveKvClientPool::new(
            pool_config,
            KvClientPoolOptions {
                connect_timeout: Default::default(),
                connect_throttle_period: Default::default(),
                orphan_handler: Arc::new(orphan_tx),
            },
        )
        .await;

        let client = pool.get_client().await.unwrap();

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

        client.close().await.unwrap();

        pool.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconfigure() {
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
            selected_bucket: None,
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let pool_config = KvClientPoolConfig {
            num_connections: 1,
            client_config,
        };

        let pool: NaiveKvClientPool<StdKvClient<Client>> = NaiveKvClientPool::new(
            pool_config,
            KvClientPoolOptions {
                connect_timeout: Default::default(),
                connect_throttle_period: Default::default(),
                orphan_handler: Arc::new(orphan_tx),
            },
        )
        .await;

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

        let client = pool.get_client().await.unwrap();
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
            .await;
        if result.is_ok() {
            panic!("result did not contain an error");
        }

        pool.reconfigure(KvClientPoolConfig {
            num_connections: 1,
            client_config,
        })
        .await
        .unwrap();

        let client = pool.get_client().await.unwrap();

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

        pool.close().await.unwrap();
    }
}
