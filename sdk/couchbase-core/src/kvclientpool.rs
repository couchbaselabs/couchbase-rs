use std::future::Future;
use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{broadcast, Mutex};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, sleep};

use crate::error::CoreError;
use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions};
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::packet::ResponsePacket;
use crate::result::CoreResult;

pub(crate) trait KvClientPool<K>: Sized + Send + Sync {
    fn new(
        config: KvClientPoolConfig,
        opts: KvClientPoolOptions,
    ) -> impl Future<Output = Self> + Send;
    fn get_client(&self) -> impl Future<Output = CoreResult<Arc<K>>> + Send;
}

#[derive(Debug)]
pub(crate) struct KvClientPoolConfig {
    pub num_connections: usize,
    pub client_config: Arc<KvClientConfig>,
}

pub(crate) struct KvClientPoolOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: Arc<UnboundedSender<ResponsePacket>>,
}

type KvClientsList<K> = Vec<Arc<K>>;

struct KvClients<K>
where
    K: KvClient,
{
    clients: KvClientsList<K>,

    client_idx: usize,

    connect_error: Option<CoreError>,
    connect_error_time: Option<Instant>,
}

pub(crate) struct NaiveKvClientPool<K: KvClient> {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,

    config: KvClientPoolConfig,

    orphan_handler: Arc<UnboundedSender<ResponsePacket>>,

    connect_error: Option<CoreError>,
    connect_error_time: Option<Instant>,

    close_tx: Sender<()>,
    close_rx: Receiver<()>,
    closed: AtomicBool,

    clients: Arc<Mutex<KvClients<K>>>,
}

impl<K> NaiveKvClientPool<K>
where
    K: KvClient + PartialEq + Sync + Send + 'static,
{
    async fn check_connections(&self) {
        let mut clients = self.clients.lock().await;
        let num_wanted_clients = self.config.num_connections;
        let num_active_clients = clients.clients.len();

        if num_active_clients > num_wanted_clients {
            let mut num_excess_clients = num_active_clients - num_wanted_clients;
            let mut num_closed_clients = 0;

            while num_excess_clients > 0 {
                let client_to_close = clients.clients.remove(0);
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
                        clients.connect_error = None;
                        clients.connect_error_time = None;
                        clients.clients.push(Arc::new(client));
                        num_needed_clients -= 1;
                    }
                    Err(e) => {
                        clients.connect_error_time = Some(Instant::now());
                        clients.connect_error = Some(e);
                    }
                }
            }
        }
    }

    async fn start_new_client(&self) -> CoreResult<K> {
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
            },
        )
        .await;

        if self.closed.load(Ordering::SeqCst) {
            if let Ok(mut client) = client_result {
                // TODO: Log something?
                client.close().await.unwrap_or_default();
            }

            return Err(CoreError {
                msg: "closed".to_string(),
            });
        }

        client_result
    }

    async fn get_client_slow(&self) -> CoreResult<Arc<K>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(CoreError {
                msg: "closed".to_string(),
            });
        }
        let mut clients = self.clients.lock().await;

        if !clients.clients.is_empty() {
            let idx = clients.client_idx;
            clients.client_idx += 1;
            // TODO: is this unwrap ok? It should be...
            let client = clients.clients.get(idx % clients.clients.len()).unwrap();
            return Ok(client.clone());
        }

        if let Some(e) = &clients.connect_error {
            return Err(CoreError { msg: e.to_string() });
        }

        drop(clients);

        self.check_connections().await;
        Box::pin(self.get_client_slow()).await
    }

    async fn shutdown_client(&self, client: Arc<K>) {
        let mut clients = self.clients.lock().await;

        let idx = clients.clients.iter().position(|x| *x == client);
        if let Some(idx) = idx {
            clients.clients.remove(idx);
        }

        drop(clients);

        // TODO: Should log
        client.close().await.unwrap_or_default();
    }

    // async fn handle_client_close(&self, )
}

impl<K> KvClientPool<K> for NaiveKvClientPool<K>
where
    K: KvClient + PartialEq + Sync + Send + 'static,
{
    async fn new(config: KvClientPoolConfig, opts: KvClientPoolOptions) -> Self {
        let (close_tx, close_rx) = broadcast::channel(1);
        let mut pool = NaiveKvClientPool {
            connect_timeout: opts.connect_timeout,
            connect_throttle_period: opts.connect_throttle_period,
            config,
            connect_error: None,
            connect_error_time: None,
            close_tx,
            close_rx,
            closed: AtomicBool::new(false),
            clients: Arc::new(Mutex::new(KvClients {
                clients: vec![],
                connect_error: None,
                connect_error_time: None,
                client_idx: 0,
            })),
            orphan_handler: opts.orphan_handler,
        };

        pool.check_connections().await;

        pool
    }

    async fn get_client(&self) -> CoreResult<Arc<K>> {
        let mut clients = self.clients.lock().await;

        if !clients.clients.is_empty() {
            let idx = clients.client_idx;
            clients.client_idx += 1;
            // TODO: is this unwrap ok? It should be...
            let client = clients.clients.get(idx % clients.clients.len()).unwrap();
            return Ok(client.clone());
        }

        self.get_client_slow().await
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
            client_config: Arc::new(client_config),
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
    }
}
