use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::{Deref, Sub};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::connection_state::ConnectionState;
use crate::error;
use crate::error::Result;
use crate::error::{Error, ErrorKind};
use crate::kvclient::{
    KvClient, KvClientConfig, KvClientOptions, OnErrMapFetchedHandler, OnKvClientCloseHandler,
    UnsolicitedPacketSender,
};
use crate::kvclient_ops::KvClientOps;
use crate::memdx::dispatcher::{Dispatcher, OrphanResponseHandler, UnsolicitedPacketHandler};
use arc_swap::ArcSwap;
use futures::executor::block_on;
use log::{debug, warn};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, Mutex, MutexGuard, Notify};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
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
    ) -> impl Future<Output = Result<HashMap<String, KvClientPoolClient<Self::Client>>>> + Send;
    async fn get_bucket(&self) -> Option<String>;
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

#[derive(Debug)]
pub(crate) struct KvClientPoolClient<K: KvClient> {
    pub client: Option<Arc<K>>,
    pub connection_state: ConnectionState,
}

impl<K: KvClient> Clone for KvClientPoolClient<K> {
    fn clone(&self) -> Self {
        KvClientPoolClient {
            client: self.client.clone(),
            connection_state: self.connection_state,
        }
    }
}

impl<K: KvClient> KvClientPoolClient<K> {
    pub fn new() -> Self {
        KvClientPoolClient {
            client: None,
            connection_state: ConnectionState::Disconnected,
        }
    }

    pub fn with_connection_state(connection_state: ConnectionState) -> Self {
        KvClientPoolClient {
            client: None,
            connection_state,
        }
    }

    pub fn with_client(client: Arc<K>, connection_state: ConnectionState) -> Self {
        KvClientPoolClient {
            client: Some(client),
            connection_state,
        }
    }
}

type KvClientPoolClients<K> = HashMap<String, KvClientPoolClient<K>>;

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
    shutdown_token: CancellationToken,
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
                if let Some(client) = &client.client {
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
                state
                    .clients
                    .insert(client_id.clone(), KvClientPoolClient::new());

                self.start_new_client(config, client_id).await;
            }
        }

        if num_needed_clients < 0 {
            let num_excess_clients = num_clients - num_wanted_clients;
            let mut clients = &mut state.clients;
            let mut ids_to_remove = vec![];
            for (id, client) in clients.iter_mut() {
                if let Some(cli) = &client.client {
                    client.connection_state = ConnectionState::Disconnecting;
                    cli.close().await.unwrap_or_default();
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
        debug!("Reconfiguring NaiveKvClientPool");
        self.num_connections_wanted
            .store(config.num_connections, Ordering::SeqCst);

        {
            let mut state = self.state.lock().await;
            if state.current_config != config.client_config {
                let mut clients_to_remove = Vec::new();
                for (id, client) in state.clients.iter_mut() {
                    if let Some(cli) = &client.client {
                        client.connection_state = ConnectionState::Connecting;
                        let res = select! {
                            _ = self.shutdown_token.cancelled() => {
                                debug!("Shutdown notified");
                                return Ok(());
                            }
                            res = cli.reconfigure(config.client_config.clone()) => res
                        };

                        if let Err(e) = res {
                            // TODO: log here.
                            dbg!(e);
                            cli.close().await.unwrap_or_default();
                            client.connection_state = ConnectionState::Disconnected;
                            clients_to_remove.push(id.clone());
                            continue;
                        };

                        client.connection_state = ConnectionState::Connected;
                    }
                }

                for id in clients_to_remove {
                    state.clients.remove(&id);
                }
            }
        }

        self.check_connections().await;
        self.rebuild_fast_map().await;

        Ok(())
    }

    pub async fn get_all_clients(&self) -> Result<HashMap<String, KvClientPoolClient<K>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        let guard = self.state.lock().await;
        let mut clients = HashMap::new();
        for (id, client) in &guard.clients {
            clients.insert(id.clone(), client.clone());
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
                .filter_map(|(_id, c)| c.client.as_ref().map(|client| client.clone()))
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
            if let Some(client) = &client.client {
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

    async fn maybe_throttle_on_error(
        throttle_period: Duration,
        guard: &mut MutexGuard<'_, KvClientPoolClientState<K>>,
        shutdown_token: &CancellationToken,
    ) -> Result<()> {
        if let Some(e) = &guard.connection_error {
            let elapsed = e.connect_error_time.elapsed();
            if elapsed < throttle_period {
                let to_sleep = throttle_period.sub(elapsed);
                debug!("Throttling new connection attempt for {:?}", to_sleep);
                return select! {
                    _ = shutdown_token.cancelled() => {
                        debug!("Shutdown notified");
                        Err(ErrorKind::Shutdown.into())
                    }
                    _ = sleep(to_sleep) => Ok(()),
                };
            }
        }

        Ok(())
    }

    fn update_client_state_if_exists(
        mut guard: MutexGuard<'_, KvClientPoolClientState<K>>,
        id: &str,
        state: ConnectionState,
    ) {
        if let Some(client) = guard.clients.get_mut(id) {
            client.connection_state = state;
        }
    }

    async fn create_client_with_shutdown(
        config: KvClientConfig,
        opts: KvClientOptions,
        shutdown_token: &CancellationToken,
    ) -> Result<K> {
        select! {
            _ = shutdown_token.cancelled() => {
                debug!("Shutdown notified");
                Err(ErrorKind::Shutdown.into())
            }
            c = K::new(config, opts) => c,
        }
    }

    async fn start_new_client_thread(
        state: Arc<Mutex<KvClientPoolClientState<K>>>,
        throttle_period: Duration,
        id: String,
        config: KvClientConfig,
        opts: KvClientOptions,
        shutdown_token: CancellationToken,
        on_new_client_tx: broadcast::Sender<()>,
    ) {
        loop {
            {
                let mut guard = state.lock().await;
                if Self::maybe_throttle_on_error(throttle_period, &mut guard, &shutdown_token)
                    .await
                    .is_err()
                {
                    debug!("Client pool shutdown during connection throttling");
                    return;
                };

                Self::update_client_state_if_exists(guard, &id, ConnectionState::Connecting);
            }

            match Self::create_client_with_shutdown(config.clone(), opts.clone(), &shutdown_token)
                .await
            {
                Ok(r) => {
                    debug!("New client created successfully: {}", r.id());
                    let new_config = {
                        let mut guard = state.lock().await;
                        guard.connection_error = None;

                        if config == guard.current_config {
                            let mut clients = &mut guard.clients;
                            if let Some(mut client) = clients.get_mut(r.id()) {
                                debug!("Changing client {} connection state to Connected", r.id());
                                client.connection_state = ConnectionState::Connected;
                                client.client = Some(Arc::new(r));

                                drop(guard);

                                match on_new_client_tx.send(()) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!("Error sending new client notification: {e}");
                                    }
                                };
                            } else {
                                drop(guard);

                                match r.close().await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        debug!("Error closing client {}: {}", r.id(), e);
                                    }
                                }

                                continue;
                            };

                            return;
                        }

                        guard.current_config.clone()
                    };

                    match r.reconfigure(new_config).await {
                        Ok(_) => {
                            debug!("Reconfigured client {} to new config", r.id());
                            let mut guard = state.lock().await;
                            let mut clients = &mut guard.clients;
                            if let Some(mut client) = clients.get_mut(r.id()) {
                                client.connection_state = ConnectionState::Connected;
                                client.client = Some(Arc::new(r));

                                drop(guard);

                                match on_new_client_tx.send(()) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!("Error sending new client notification: {e}");
                                    }
                                };

                                return;
                            } else {
                                drop(guard);

                                match r.close().await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        debug!("Error closing client {}: {}", r.id(), e);
                                    }
                                }

                                continue;
                            };
                        }
                        Err(e) => {
                            let mut msg = format!("Failed to reconfigure client {}: {}", r.id(), e);
                            if let Some(source) = e.source() {
                                msg = format!("{msg} - {source}");
                            }
                            debug!("{msg}");

                            match r.close().await {
                                Ok(_) => {}
                                Err(e) => {
                                    debug!("Error closing client {}: {}", r.id(), e);
                                }
                            }

                            continue;
                        }
                    };
                }
                Err(e) => {
                    let mut msg = format!("Error creating new client {e}");
                    if *e.kind() == ErrorKind::Shutdown {
                        return;
                    }

                    if let Some(source) = e.source() {
                        msg = format!("{msg} - {source}");
                    }
                    debug!("{msg}");

                    let mut guard = state.lock().await;

                    guard.connection_error = Some(ConnectionError {
                        connect_error: e,
                        connect_error_time: Instant::now(),
                    });
                }
            }
        }
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
        let throttle_period = self.connect_throttle_period;

        let tx = self.new_client_watcher_tx.clone();
        let shutdown_token = self.shutdown_token.child_token();

        tokio::spawn(Self::start_new_client_thread(
            state,
            throttle_period,
            id,
            config,
            opts,
            shutdown_token,
            tx,
        ));
    }

    async fn get_bucket(&self) -> Option<String> {
        let state = self.state.lock().await;

        state.current_config.selected_bucket.clone()
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
            clients.insert(Uuid::new_v4().to_string(), KvClientPoolClient::new());
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
            shutdown_token: CancellationToken::new(),
        });

        {
            let inner_clone = Arc::downgrade(&inner);
            tokio::spawn(async move {
                let mut on_client_close_rx = on_client_close_rx;
                while let Some(client_id) = on_client_close_rx.recv().await {
                    if let Some(inner) = inner_clone.upgrade() {
                        inner.handle_client_close(client_id).await;
                    } else {
                        debug!("Client close handler exited");
                        return;
                    }
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

    async fn shutdown_client(&self, client: Arc<K>) {
        self.inner.shutdown_client(client).await;
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    async fn reconfigure(&self, config: KvClientPoolConfig) -> Result<()> {
        self.inner.reconfigure(config).await
    }

    async fn get_all_clients(&self) -> Result<HashMap<String, KvClientPoolClient<Self::Client>>> {
        self.inner.get_all_clients().await
    }

    async fn get_bucket(&self) -> Option<String> {
        self.inner.get_bucket().await
    }
}

impl<K> Drop for KvClientPoolClientInner<K>
where
    K: KvClient,
{
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}
