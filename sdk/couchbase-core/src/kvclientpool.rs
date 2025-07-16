use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Sub};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::error::{Error, ErrorKind};
use crate::kvclient::{
    KvClient, KvClientConfig, KvClientOptions, OnErrMapFetchedHandler, OnKvClientCloseHandler,
    UnsolicitedPacketSender,
};
use crate::kvclient_ops::KvClientOps;
use crate::memdx::dispatcher::{Dispatcher, OrphanResponseHandler, UnsolicitedPacketHandler};
use arc_swap::ArcSwap;
use log::debug;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, Mutex, MutexGuard, Notify};
use tokio::time::{sleep, Instant};
use urlencoding::decode_binary;
use uuid::Uuid;
// TODO: This needs some work, some more thought should go into the locking strategy as it's possible
// there are still races in this. Additionally it's extremely easy to write in deadlocks.

pub(crate) trait KvClientPool: Sized + Send + Sync {
    type Client: KvClient + KvClientOps + Send + Sync;

    fn new(
        config: KvClientPoolConfig,
        opts: KvClientPoolOptions,
    ) -> impl Future<Output = Self> + Send;
    fn get_client(&self) -> impl Future<Output = Result<Arc<Self::Client>>> + Send;
    fn shutdown_client(&self, client: Arc<Self::Client>) -> impl Future<Output = ()> + Send;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
    fn reconfigure(&self, config: KvClientPoolConfig) -> impl Future<Output = Result<()>> + Send;
    fn get_all_clients(
        &self,
    ) -> impl Future<Output = Result<Vec<Option<Arc<Self::Client>>>>> + Send;
}

#[derive(Clone)]
pub(crate) struct KvClientPoolConfig {
    pub num_connections: usize,
    pub client_config: KvClientConfig,
}

pub(crate) struct KvClientPoolOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    pub orphan_handler: OrphanResponseHandler,
    pub on_err_map_fetched: Option<OnErrMapFetchedHandler>,
    pub disable_decompression: bool,
}

#[derive(Debug, Clone)]
struct ConnectionError {
    pub connect_error: Error,
    pub connect_error_time: Instant,
}

type KvClientPoolClients<K> = HashMap<String, Option<Arc<K>>>;

struct KvClientPoolClientState<K: KvClient> {
    current_config: KvClientConfig,

    connection_error: Option<ConnectionError>,

    clients: KvClientPoolClients<K>,
    reconfiguring_clients: KvClientPoolClients<K>,
}

struct KvClientPoolClientInner<K: KvClient> {
    connect_timeout: Duration,
    connect_throttle_period: Duration,

    unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    orphan_handler: OrphanResponseHandler,
    on_client_close_tx: Sender<String>,
    on_err_map_fetched: Option<OnErrMapFetchedHandler>,

    disable_decompression: bool,

    num_connections_wanted: AtomicUsize,
    fast_map: ArcSwap<Vec<Arc<K>>>,

    state: Arc<Mutex<KvClientPoolClientState<K>>>,
    client_idx: AtomicUsize,

    new_client_watcher_tx: broadcast::Sender<()>,
    // We hold onto this to prevent the sender from erroring.
    new_client_watcher_rx: broadcast::Receiver<()>,

    closed: AtomicBool,
    shutdown_notify: Arc<Notify>,
}

pub(crate) struct NaiveKvClientPool<K: KvClient> {
    inner: Arc<KvClientPoolClientInner<K>>,
}

impl<K> KvClientPoolClientInner<K>
where
    K: KvClient + KvClientOps + PartialEq + Sync + Send + 'static,
{
    pub async fn get_client(&self) -> Result<Arc<K>> {
        {
            let fm = self.fast_map.load();

            if !fm.is_empty() {
                let idx = self.client_idx.fetch_add(1, Ordering::SeqCst);
                if let Some(client) = fm.get(idx % fm.len()) {
                    return Ok(client.clone());
                }
            }
        }

        self.get_client_slow().await
    }

    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        {
            let mut state = self.state.lock().await;
            for (_id, client) in state.clients.iter() {
                // TODO: probably log
                if let Some(client) = &client {
                    client.close().await.unwrap_or_default();
                }
            }

            state.clients = HashMap::new();
        }

        Ok(())
    }

    pub async fn check_connections(&self) {
        let mut state = self.state.lock().await;
        let num_clients = state.clients.len() as isize;
        let num_wanted_clients = self.num_connections_wanted.load(Ordering::SeqCst) as isize;
        let num_needed_clients = num_wanted_clients - num_clients;

        if num_needed_clients > 0 {
            for _ in 0..num_needed_clients {
                let client_id = Uuid::new_v4().to_string();
                let config = state.current_config.clone();
                debug!("Creating new client with id {}", &client_id);
                state.clients.insert(client_id.clone(), None);

                self.start_new_client(config, client_id).await;
            }
        }

        if num_needed_clients < 0 {
            let num_excess_clients = num_clients - num_wanted_clients;
            let mut clients = &mut state.clients;
            let mut ids_to_remove = vec![];
            for (id, client) in clients.iter() {
                if let Some(client) = &client {
                    client.close().await.unwrap_or_default();
                    ids_to_remove.push(id.clone());
                }

                if ids_to_remove.len() >= num_excess_clients as usize {
                    break;
                }
            }
            for id in ids_to_remove {
                clients.remove(&id);
            }
        }
    }

    pub async fn reconfigure(&self, config: KvClientPoolConfig) -> Result<()> {
        self.num_connections_wanted
            .store(config.num_connections, Ordering::SeqCst);

        {
            let mut state = self.state.lock().await;
            let mut new_clients = HashMap::new();
            for (id, client) in state.clients.iter() {
                if let Some(client) = &client {
                    let res = select! {
                        _ = self.shutdown_notify.notified() => {
                            debug!("Shutdown notified");
                            return Ok(());
                        }
                        res = client.reconfigure(config.client_config.clone()) => res
                    };

                    if let Err(e) = res {
                        // TODO: log here.
                        dbg!(e);
                        client.close().await.unwrap_or_default();
                        continue;
                    };
                }

                new_clients.insert(id.clone(), client.clone());
            }

            state.clients = new_clients;
        }

        self.check_connections().await;

        Ok(())
    }

    pub async fn get_all_clients(&self) -> Result<Vec<Option<Arc<K>>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        let guard = self.state.lock().await;
        let mut clients = Vec::with_capacity(guard.clients.len());
        for client in guard.clients.values() {
            clients.push(client.clone());
        }

        Ok(clients)
    }

    async fn get_client_slow(&self) -> Result<Arc<K>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        {
            let state = self.state.lock().await;
            let clients = &state.clients;
            let available_clients: Vec<_> = clients
                .iter()
                .filter_map(|(_id, c)| c.as_ref().map(|client| client.clone()))
                .collect();
            if !available_clients.is_empty() {
                let idx = self.client_idx.fetch_add(1, Ordering::SeqCst);
                if let Some(client) = available_clients.get(idx % available_clients.len()) {
                    return Ok(client.clone());
                };
            }

            if let Some(e) = &state.connection_error {
                return Err(e.connect_error.clone());
            }
        }

        let mut rx = self.new_client_watcher_tx.subscribe();
        let _ = rx.recv().await;
        Box::pin(self.get_client_slow()).await
    }

    pub async fn handle_client_close(&self, client_id: String) {
        debug!("Client id {} closed", &client_id);

        // TODO: not sure the ordering of close leading to here is great.
        if self.closed.load(Ordering::SeqCst) {
            debug!("Pool is closed, ignoring client close for {}", &client_id);
            return;
        }

        {
            let mut state = self.state.lock().await;
            let mut clients = &mut state.clients;
            // If the client is not in the pool, we don't need to do anything.
            clients.remove(&client_id);
        }

        self.check_connections().await;
        self.rebuild_fast_map().await;
    }

    async fn rebuild_fast_map(&self) {
        let state = self.state.lock().await;
        let clients = &state.clients;
        let mut new_map = Vec::new();
        for client in clients.values() {
            if let Some(client) = &client {
                new_map.push(client.clone());
            }
        }
        self.fast_map.store(Arc::from(new_map));
    }

    pub async fn shutdown_client(&self, client: Arc<K>) {
        {
            let mut state = self.state.lock().await;
            let mut clients = &mut state.clients;
            clients.remove(client.id());
        }

        self.rebuild_fast_map().await;

        // TODO: Should log
        client.close().await.unwrap_or_default();
    }

    async fn start_new_client(&self, config: KvClientConfig, id: String) {
        let on_client_close_tx = self.on_client_close_tx.clone();
        let state = self.state.clone();
        let opts = KvClientOptions {
            unsolicited_packet_tx: self.unsolicited_packet_tx.clone(),
            orphan_handler: self.orphan_handler.clone(),
            on_close: Arc::new(move |client_id| {
                let on_client_close_tx = on_client_close_tx.clone();
                Box::pin(async move {
                    if let Err(e) = on_client_close_tx.send(client_id).await {
                        debug!("Failed to send client close notification: {e}");
                    }
                })
            }),
            disable_decompression: self.disable_decompression,
            on_err_map_fetched: self.on_err_map_fetched.clone(),
            id: id.clone(),
        };

        let tx = self.new_client_watcher_tx.clone();
        let notify = self.shutdown_notify.clone();

        tokio::spawn(async move {
            {
                let guard = state.lock().await;
                if let Some(e) = &guard.connection_error {
                    // TODO(RSCBC-52): Make configurable.
                    sleep(Duration::from_millis(5000)).await;
                }
            }

            let client_result = select! {
                _ = notify.notified() => {
                    debug!("Shutdown notified");
                    return;
                }
                c = K::new(config.clone(), opts) => c,
            };

            match client_result {
                Ok(r) => {
                    debug!("New client created successfully");
                    let mut guard = state.lock().await;
                    guard.connection_error = None;

                    if config != guard.current_config {
                        match r.reconfigure(guard.current_config.clone()).await {
                            Ok(_) => {
                                debug!("Reconfigured client {} to new config", r.id());
                                let mut clients = &mut guard.clients;
                                if let Some(mut client) = clients.get(r.id()) {
                                    if client.is_none() {
                                        clients.insert(r.id().to_string(), Some(Arc::new(r)));
                                    }
                                } else {
                                    // TODO: handle this.
                                    let _ = r.close().await;
                                };
                            }
                            Err(e) => {
                                debug!("Failed to reconfigure client {}: {}", r.id(), e);
                                let mut clients = &mut guard.clients;
                                // It doesn't matter if this isn't in clients, we just want to make sure
                                // it isn't.
                                clients.remove(r.id());
                            }
                        };

                        return;
                    }

                    let mut clients = &mut guard.clients;
                    if let Some(mut client) = clients.get(r.id()) {
                        if client.is_none() {
                            // insert will actually perform an update here.
                            clients.insert(r.id().to_string(), Some(Arc::new(r)));
                        }
                    } else {
                        // TODO: handle this.
                        let _ = r.close().await;
                    };
                }
                Err(e) => {
                    debug!("Error creating new client: {}", &e);
                    let mut guard = state.lock().await;

                    guard.connection_error = Some(ConnectionError {
                        connect_error: e,
                        connect_error_time: Instant::now(),
                    });

                    guard.clients.remove(&id);
                }
            }

            // TODO: something
            let _ = tx.send(());
        });
    }
}

impl<K> KvClientPool for NaiveKvClientPool<K>
where
    K: KvClient + KvClientOps + PartialEq + Sync + Send + 'static,
{
    type Client = K;

    async fn new(config: KvClientPoolConfig, opts: KvClientPoolOptions) -> Self {
        let (on_client_close_tx, on_client_close_rx) = tokio::sync::mpsc::channel(1);

        let mut clients = HashMap::with_capacity(config.num_connections);
        for _ in 0..config.num_connections {
            clients.insert(Uuid::new_v4().to_string(), None);
        }

        let (new_client_watcher_tx, new_client_watcher_rx) = broadcast::channel(1);

        let mut inner = Arc::new(KvClientPoolClientInner {
            connect_timeout: opts.connect_timeout,
            connect_throttle_period: opts.connect_throttle_period,

            num_connections_wanted: AtomicUsize::new(config.num_connections),
            client_idx: AtomicUsize::new(0),
            fast_map: ArcSwap::from_pointee(vec![]),

            state: Arc::new(Mutex::new(KvClientPoolClientState {
                current_config: config.client_config,
                connection_error: None,
                clients,
                reconfiguring_clients: Default::default(),
            })),

            unsolicited_packet_tx: opts.unsolicited_packet_tx,
            orphan_handler: opts.orphan_handler.clone(),
            on_client_close_tx,
            on_err_map_fetched: opts.on_err_map_fetched,

            disable_decompression: opts.disable_decompression,

            new_client_watcher_tx,
            new_client_watcher_rx,

            closed: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
        });

        {
            let inner_clone = inner.clone();
            tokio::spawn(async move {
                let mut on_client_close_rx = on_client_close_rx;
                while let Some(client_id) = on_client_close_rx.recv().await {
                    inner_clone.handle_client_close(client_id).await;
                }
            });
        }

        {
            let state = inner.state.lock().await;
            for id in state.clients.keys() {
                let config = state.current_config.clone();
                inner.start_new_client(config, id.clone()).await;
            }
        }

        inner.check_connections().await;

        NaiveKvClientPool { inner }
    }

    async fn get_client(&self) -> Result<Arc<K>> {
        self.inner.get_client().await
    }

    async fn get_all_clients(&self) -> Result<Vec<Option<Arc<Self::Client>>>> {
        self.inner.get_all_clients().await
    }

    async fn shutdown_client(&self, client: Arc<K>) {
        self.inner.shutdown_client(client).await;
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn reconfigure(&self, config: KvClientPoolConfig) -> Result<()> {
        self.inner.reconfigure(config).await
    }
}
