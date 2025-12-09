/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::{Deref, Sub};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::authenticator::Authenticator;
use crate::connection_state::ConnectionState;
use crate::error;
use crate::error::Result;
use crate::error::{Error, ErrorKind};
use crate::kvclient::{
    KvClient, KvClientBootstrapOptions, KvClientOptions, OnErrMapFetchedHandler,
    OnKvClientCloseHandler, UnsolicitedPacketSender,
};
use crate::kvclient_babysitter::{KvClientBabysitter, KvClientBabysitterOptions, KvTarget};
use crate::kvclient_ops::KvClientOps;
use crate::memdx::dispatcher::{Dispatcher, OrphanResponseHandler, UnsolicitedPacketHandler};
use crate::memdx::request::PingRequest;
use crate::memdx::response::PingResponse;
use crate::results::diagnostics::EndpointDiagnostics;
use arc_swap::ArcSwap;
use futures::executor::block_on;
use futures::future::join_all;
use log::{debug, error, info, warn};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, Mutex, MutexGuard, Notify};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
use urlencoding::decode_binary;
use uuid::Uuid;

pub(crate) trait KvClientPool: Send + Sync {
    type Client: KvClient + KvClientOps + Send + Sync;

    fn new(opts: KvClientPoolOptions) -> impl Future<Output = Self> + Send;
    fn id(&self) -> &str;
    fn get_client(&self) -> impl Future<Output = Result<Arc<Self::Client>>> + Send;
    fn ping_all_clients(
        &self,
        req: PingRequest,
    ) -> impl Future<Output = Vec<Result<PingResponse>>> + Send;
    fn endpoint_diagnostics(&self) -> impl Future<Output = Vec<EndpointDiagnostics>> + Send;
    fn update_auth(&self, authenticator: Authenticator) -> impl Future<Output = ()> + Send;
    fn update_target(&self, target: KvTarget) -> impl Future<Output = ()> + Send;
    // async fn update_selected_bucket(&self, bucket_name: String);
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct KvClientPoolOptions {
    pub num_connections: usize,
    pub connect_throttle_period: Duration,
    pub disable_decompression: bool,
    pub bootstrap_options: KvClientBootstrapOptions,
    pub endpoint_id: String,
    pub on_demand_connect: bool,

    pub target: KvTarget,
    pub auth: Authenticator,
    pub selected_bucket: Option<String>,

    pub unsolicited_packet_tx: Option<UnsolicitedPacketSender>,
    pub orphan_handler: Option<OrphanResponseHandler>,
}

struct KvClientPoolFastMap<K> {
    clients: Vec<Arc<K>>,
}

#[derive(Clone)]
struct KvClientPoolEntry<B, K>
where
    B: KvClientBabysitter,
    K: KvClient + KvClientOps,
{
    babysitter: Arc<B>,
    client: Option<Arc<K>>,
}

pub(crate) struct StdKvClientPool<B, K>
where
    B: KvClientBabysitter,
    K: KvClient + KvClientOps,
{
    id: String,

    client_idx: AtomicUsize,
    fast_map: Arc<ArcSwap<KvClientPoolFastMap<K>>>,

    babysitters: Arc<Mutex<Vec<KvClientPoolEntry<B, K>>>>,
}

impl<B, K> KvClientPool for StdKvClientPool<B, K>
where
    B: KvClientBabysitter<Client = K> + Send + 'static + std::marker::Sync,
    K: KvClient + KvClientOps + 'static,
{
    type Client = K;

    async fn new(opts: KvClientPoolOptions) -> Self {
        let id = Uuid::new_v4().to_string();
        debug!(
            "Creating new client pool {} for {} - {:?}",
            &id, &opts.target.address, &opts.selected_bucket
        );

        let fast_map = Arc::new(ArcSwap::from_pointee(KvClientPoolFastMap {
            clients: vec![],
        }));

        let babysitters: Arc<Mutex<Vec<KvClientPoolEntry<B, K>>>> =
            Arc::new(Mutex::new(Vec::with_capacity(opts.num_connections)));

        let babysitters_clone = babysitters.clone();
        let fast_map_clone = fast_map.clone();

        {
            let mut babysitters_guard = babysitters.lock().await;
            for idx in 0..opts.num_connections {
                let babysitters_clone = babysitters_clone.clone();
                let fast_map_clone = fast_map_clone.clone();
                let babysitter = KvClientBabysitter::new(KvClientBabysitterOptions {
                    id: Uuid::new_v4().to_string(),
                    endpoint_id: opts.endpoint_id.clone(),
                    on_demand_connect: opts.on_demand_connect,

                    connect_throttle_period: opts.connect_throttle_period,
                    disable_decompression: opts.disable_decompression,
                    bootstrap_opts: opts.bootstrap_options.clone(),
                    state_change_handler: Arc::new(move |babysitter_id, client, error| {
                        let babysitters_clone = babysitters_clone.clone();
                        let fast_map_clone = fast_map_clone.clone();
                        Box::pin(async move {
                            let mut guard = babysitters_clone.lock().await;

                            let entry = guard
                                .iter_mut()
                                .find(|entry| entry.babysitter.id() == babysitter_id);
                            if let Some(entry) = entry {
                                entry.client = client;
                            }

                            let mut clients = vec![];
                            for entry in guard.iter() {
                                if let Some(client) = &entry.client {
                                    clients.push(client.clone());
                                }
                            }

                            fast_map_clone.store(Arc::new(KvClientPoolFastMap { clients }));
                        })
                    }),
                    unsolicited_packet_tx: opts.unsolicited_packet_tx.clone(),
                    orphan_handler: opts.orphan_handler.clone(),
                    target: opts.target.clone(),
                    auth: opts.auth.clone(),
                    selected_bucket: opts.selected_bucket.clone(),
                });

                babysitters_guard.insert(
                    idx,
                    KvClientPoolEntry {
                        babysitter: Arc::new(babysitter),
                        client: None,
                    },
                );
            }
        }

        StdKvClientPool {
            id,
            client_idx: Default::default(),
            fast_map,
            babysitters,
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn get_client(&self) -> Result<Arc<K>> {
        let fast_map = self.fast_map.load();
        let num_fast_map_connections = fast_map.clients.len();
        if num_fast_map_connections > 0 {
            let client_idx = self.client_idx.fetch_add(1, Ordering::Relaxed);
            let client = fast_map.clients[client_idx % num_fast_map_connections].clone();
            return Ok(client);
        }

        debug!("Client pool {} no client found in fast map", self.id);

        self.get_client_slow().await
    }

    async fn ping_all_clients(&self, req: PingRequest<'_>) -> Vec<Result<PingResponse>> {
        let mut babysitters = vec![];
        {
            let guard = self.babysitters.lock().await;

            for babysitter_entry in guard.iter() {
                babysitters.push(babysitter_entry.babysitter.clone())
            }
        }

        let mut pool_handles = Vec::with_capacity(babysitters.len());
        for babysitter in babysitters {
            let req = req.clone();
            let handle = async move {
                let client = babysitter.get_client().await?;
                client
                    .ping(req)
                    .await
                    .map_err(Error::new_contextual_memdx_error)
            };

            pool_handles.push(handle);
        }

        join_all(pool_handles).await
    }

    async fn endpoint_diagnostics(&self) -> Vec<EndpointDiagnostics> {
        let babysitters = self.babysitters.lock().await;

        let mut diags = vec![];
        for babysitter_entry in babysitters.iter() {
            diags.push(babysitter_entry.babysitter.endpoint_diagnostics());
        }

        diags
    }

    async fn update_auth(&self, authenticator: Authenticator) {
        let babysitters = self.babysitters.lock().await;
        for babysitter_entry in babysitters.iter() {
            babysitter_entry
                .babysitter
                .update_auth(authenticator.clone())
                .await;
        }
    }

    async fn update_target(&self, target: KvTarget) {
        let babysitters = self.babysitters.lock().await;
        for babysitter_entry in babysitters.iter() {
            babysitter_entry
                .babysitter
                .update_target(target.clone())
                .await;
        }
    }

    async fn close(&self) -> Result<()> {
        info!("Closing pool {}", self.id);

        self.fast_map
            .swap(Arc::new(KvClientPoolFastMap { clients: vec![] }));

        let mut babysitters = self.babysitters.lock().await;
        for babysitter_entry in babysitters.drain(..) {
            if let Err(e) = babysitter_entry.babysitter.close().await {
                debug!("Failed to close babysitter: {e:?}");
            }
        }

        Ok(())
    }
}

impl<B, K> StdKvClientPool<B, K>
where
    B: KvClientBabysitter<Client = K>,
    K: KvClient + KvClientOps,
{
    async fn get_client_slow(&self) -> Result<Arc<K>> {
        let babysitter = {
            let babysitters = self.babysitters.lock().await;
            let client_idx = self.client_idx.fetch_add(1, Ordering::Relaxed);

            babysitters[client_idx % babysitters.len()]
                .babysitter
                .clone()
        };

        debug!("Client pool {} no client found in slow map", self.id);

        babysitter.get_client().await
    }
}
