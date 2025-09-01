use crate::configmanager::ConfigManager;
use crate::connection_state::ConnectionState;
use crate::error::Error;
use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::KvClientManager;
use crate::kvclientpool::{KvClientPool, KvClientPoolClient};
use crate::memdx::request::PingRequest;
use crate::options::diagnostics::DiagnosticsOptions;
use crate::options::ping::PingOptions;
use crate::options::waituntilready::{ClusterState, WaitUntilReadyOptions};
use crate::querycomponent::QueryComponent;
use crate::results::diagnostics::{DiagnosticsResult, EndpointDiagnostics};
use crate::results::pingreport::{EndpointPingReport, PingReport, PingState};
use crate::retry::{RetryInfo, RetryManager, RetryReason};
use crate::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use crate::searchcomponent::SearchComponent;
use crate::service_type::ServiceType;
use chrono::Utc;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use futures_core::future::BoxFuture;
use log::debug;
use serde::ser::SerializeStruct;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::ops::Sub;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub(crate) struct PingQueryReportOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub(crate) timeout: Duration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PingSearchReportOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub(crate) timeout: Duration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PingKvOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a str>,
    pub(crate) timeout: Duration,
    pub(crate) bucket: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
struct PingEveryKvConnectionOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a str>,
}

pub struct DiagnosticsComponent<C: Client, M: KvClientManager> {
    kv_client_manager: Arc<M>,
    query_component: Arc<QueryComponent<C>>,
    search_component: Arc<SearchComponent<C>>,

    state: Mutex<DiagnosticsComponentState>,

    retry_manager: Arc<RetryManager>,
}

#[derive(Debug)]
pub(crate) struct DiagnosticsComponentConfig {
    pub bucket: Option<String>,
    pub services: Vec<ServiceType>,
    pub rev_id: i64,
}

struct DiagnosticsComponentState {
    bucket: Option<String>,
    services: Vec<ServiceType>,
    rev_id: i64,
}

impl<C: Client + 'static, M: KvClientManager> DiagnosticsComponent<C, M> {
    pub fn new(
        kv_client_manager: Arc<M>,
        query_component: Arc<QueryComponent<C>>,
        search_component: Arc<SearchComponent<C>>,
        retry_manager: Arc<RetryManager>,
        config: DiagnosticsComponentConfig,
    ) -> Self {
        let state = Mutex::new(DiagnosticsComponentState {
            bucket: config.bucket,
            services: config.services,
            rev_id: config.rev_id,
        });

        Self {
            kv_client_manager,
            query_component,
            search_component,

            retry_manager,

            state,
        }
    }

    pub fn reconfigure(&self, config: DiagnosticsComponentConfig) {
        let mut state = self.state.lock().unwrap();
        state.rev_id = config.rev_id;
        state.bucket = config.bucket.clone();
        state.services = config.services.clone();
    }

    pub async fn diagnostics(
        &self,
        _opts: &DiagnosticsOptions,
    ) -> crate::error::Result<DiagnosticsResult> {
        let pools = self.kv_client_manager.get_all_pools();

        let mut endpoint_reports = vec![];
        for (endpoint, pool) in &pools {
            for (id, client) in pool.get_all_clients().await? {
                let (local_address, last_activity) = if let Some(cli) = &client.client {
                    (
                        Some(cli.local_addr().to_string()),
                        Some(
                            Utc::now()
                                .sub(cli.last_activity().to_utc())
                                .num_microseconds()
                                .unwrap_or_default(),
                        ),
                    )
                } else {
                    (None, None)
                };

                endpoint_reports.push(EndpointDiagnostics {
                    service_type: ServiceType::MEMD,
                    id,
                    local_address,
                    remote_address: endpoint.to_string(),
                    last_activity,
                    namespace: pool.get_bucket().await,
                    state: client.connection_state,
                });
            }
        }

        Ok(DiagnosticsResult {
            version: 2,
            config_rev: self.state.lock().unwrap().rev_id,
            id: Uuid::new_v4().to_string(),
            sdk: "rust".to_string(),
            services: HashMap::from([(ServiceType::MEMD, endpoint_reports)]),
        })
    }

    pub async fn ping(&self, opts: &PingOptions) -> crate::error::Result<PingReport>
    where
        <<M as KvClientManager>::Pool as KvClientPool>::Client: 'static,
    {
        let (rev_id, bucket, available_services) = {
            let state = self.state.lock().unwrap();
            (state.rev_id, state.bucket.clone(), state.services.clone())
        };

        let service_types = if let Some(st) = &opts.service_types {
            if st.is_empty() {
                available_services
            } else {
                st.clone()
            }
        } else {
            available_services
        };

        let on_behalf_of = opts.on_behalf_of.as_ref();

        let mut services = HashMap::new();
        if service_types.contains(&ServiceType::QUERY) {
            let query_report = self
                .query_component
                .create_ping_report(PingQueryReportOptions {
                    on_behalf_of,
                    timeout: opts
                        .query_timeout
                        .unwrap_or_else(|| Duration::from_secs(75)),
                })
                .await?;
            services.insert(ServiceType::QUERY, query_report);
        }

        if service_types.contains(&ServiceType::SEARCH) {
            let search_report = self
                .search_component
                .create_ping_report(PingSearchReportOptions {
                    on_behalf_of,
                    timeout: opts
                        .search_timeout
                        .unwrap_or_else(|| Duration::from_secs(75)),
                })
                .await?;
            services.insert(ServiceType::SEARCH, search_report);
        }

        if service_types.contains(&ServiceType::MEMD) {
            let on_behalf_of = on_behalf_of.map(|b| b.username.as_str());
            let kv_report = self
                .ping_all_kv_nodes(PingKvOptions {
                    on_behalf_of,
                    timeout: opts
                        .kv_timeout
                        .unwrap_or_else(|| Duration::from_millis(2500)),
                    bucket: bucket.as_deref(),
                })
                .await?;
            services.insert(ServiceType::MEMD, kv_report);
        }

        Ok(PingReport {
            version: 2,
            id: Uuid::new_v4().to_string(),
            sdk: "rust".to_string(),
            config_rev: rev_id,
            services,
        })
    }

    pub async fn wait_until_ready(&self, opts: &WaitUntilReadyOptions) -> crate::error::Result<()> {
        let desired_state = opts.desired_state.unwrap_or(ClusterState::Online);
        if desired_state == ClusterState::Offline {
            return Err(Error::new_invalid_argument_error(
                "cannot be Offline",
                Some("desired_state".to_string()),
            ));
        }

        let mut retry_info = RetryInfo::new(
            "wait_until_ready",
            true,
            Arc::new(BestEffortRetryStrategy::new(
                ExponentialBackoffCalculator::default(),
            )),
        );

        let available_services = {
            let state = self.state.lock().unwrap();
            state.services.clone()
        };

        let service_types = if let Some(st) = &opts.service_types {
            if st.is_empty() {
                available_services
            } else {
                st.clone()
            }
        } else {
            available_services
        };

        let on_behalf_of = opts.on_behalf_of.as_ref();

        loop {
            let mut handles = FuturesUnordered::<BoxFuture<bool>>::new();
            if service_types.contains(&ServiceType::QUERY) {
                handles.push(Box::pin(
                    self.is_query_ready(opts.on_behalf_of.as_ref(), desired_state),
                ))
            }

            if service_types.contains(&ServiceType::SEARCH) {
                handles.push(Box::pin(
                    self.is_search_ready(opts.on_behalf_of.as_ref(), desired_state),
                ));
            }

            if service_types.contains(&ServiceType::MEMD) {
                let on_behalf_of = on_behalf_of.map(|b| b.username.as_str());
                handles.push(Box::pin(self.is_kv_ready(on_behalf_of, desired_state)));
            }

            let mut all_ready = true;
            while let Some(ready) = handles.next().await {
                if !ready {
                    all_ready = false;
                    break;
                }
            }

            if all_ready {
                return Ok(());
            }

            let duration = self
                .retry_manager
                .maybe_retry(&mut retry_info, RetryReason::NotReady)
                .await;

            if let Some(duration) = duration {
                debug!(
                    "Retrying {} after {:?} due to {}",
                    &retry_info,
                    duration,
                    RetryReason::NotReady
                );

                sleep(duration).await;
            } else {
                return Err(Error::new_message_error(
                    "retry manager indicated no retry, this is a bug",
                ));
            }
        }
    }

    async fn is_query_ready(
        &self,
        on_behalf_of: Option<&OnBehalfOfInfo>,
        desired_state: ClusterState,
    ) -> bool {
        let query_result = match self.query_component.ping_all_endpoints(on_behalf_of).await {
            Ok(res) => res,
            Err(_e) => {
                return false;
            }
        };

        if desired_state == ClusterState::Online {
            query_result.into_iter().all(|res| res.is_ok())
        } else {
            query_result.into_iter().any(|res| res.is_ok())
        }
    }

    async fn is_search_ready(
        &self,
        on_behalf_of: Option<&OnBehalfOfInfo>,
        desired_state: ClusterState,
    ) -> bool {
        let search_result = match self.search_component.ping_all_endpoints(on_behalf_of).await {
            Ok(res) => res,
            Err(_e) => {
                return false;
            }
        };

        if desired_state == ClusterState::Online {
            search_result.into_iter().all(|res| res.is_ok())
        } else {
            search_result.into_iter().any(|res| res.is_ok())
        }
    }

    async fn is_kv_ready(&self, on_behalf_of: Option<&str>, desired_state: ClusterState) -> bool {
        let pools = self.kv_client_manager.get_all_pools();

        let req = PingRequest { on_behalf_of };

        let mut handles = Vec::with_capacity(pools.len());
        for (pool_id, pool) in pools {
            let clients = match pool.get_all_clients().await {
                Ok(clients) => clients,
                Err(e) => {
                    debug!("Failed to get clients from pool: {e}");
                    return false;
                }
            };

            let req = req.clone();
            let handle = async move {
                debug!("Pinging pool {pool_id} with {} clients", clients.len());
                let mut pool_handles = Vec::with_capacity(clients.len());
                for (id, client) in clients {
                    let req = req.clone();

                    let handle = self.maybe_ping_client(id.clone(), req, client);

                    pool_handles.push(handle);
                }

                let results = join_all(pool_handles).await;

                if desired_state == ClusterState::Online {
                    results.into_iter().all(|ready| ready)
                } else {
                    results.into_iter().any(|ready| ready)
                }
            };

            handles.push(handle);
        }

        let results = join_all(handles).await;

        results.into_iter().all(|ready| ready)
    }

    async fn maybe_ping_client<K>(
        &self,
        pool_id: String,
        req: PingRequest<'_>,
        client: KvClientPoolClient<K>,
    ) -> bool
    where
        K: KvClient + KvClientOps,
    {
        if let Some(client) = &client.client {
            let client = client.clone();
            match client.ping(req).await {
                Ok(_) => true,
                Err(e) => {
                    debug!("Ping against client {} failed: {}", client.id(), e);
                    false
                }
            }
        } else {
            debug!("Client {pool_id} is not connected");
            false
        }
    }

    async fn ping_all_kv_nodes(
        &self,
        opts: PingKvOptions<'_>,
    ) -> crate::error::Result<Vec<EndpointPingReport>>
    where
        <<M as KvClientManager>::Pool as KvClientPool>::Client: 'static,
    {
        let clients = self.kv_client_manager.get_client_per_endpoint().await?;

        let req = PingRequest {
            on_behalf_of: opts.on_behalf_of,
        };

        let mut handles = Vec::with_capacity(clients.len());
        let bucket = { opts.bucket.map(|b| b.to_string()) };

        for client in clients {
            let client = client.clone();
            let bucket = bucket.clone();

            let req = req.clone();

            let handle = async move {
                let start = std::time::Instant::now();
                let res = client
                    .ping(req)
                    .await
                    .map_err(Error::new_contextual_memdx_error);
                let end = std::time::Instant::now();

                let (error, state) = match res {
                    Ok(_) => (None, PingState::Ok),
                    Err(e) => (Some(e), PingState::Error),
                };

                EndpointPingReport {
                    remote: client.remote_addr().to_string(),
                    error,
                    latency: end.sub(start),
                    id: Some(client.id().to_string()),
                    namespace: bucket,
                    state,
                }
            };

            handles.push(handle);
        }

        let reports = join_all(handles).await;

        Ok(reports)
    }

    async fn ping_one_kv_node(
        &self,
        client: Arc<<<M as KvClientManager>::Pool as KvClientPool>::Client>,
        timeout: Duration,
        req: PingRequest<'_>,
    ) -> EndpointPingReport {
        let start = std::time::Instant::now();
        let res = select! {
            e = tokio::time::sleep(timeout) => {
                return EndpointPingReport {
                    remote: client.remote_addr().to_string(),
                    error: None,
                    latency: std::time::Instant::now().sub(start),
                    id: None,
                    namespace: client.bucket_name(),
                    state: PingState::Timeout,
                }
            }
            r = client.ping(req) => r.map_err(Error::new_contextual_memdx_error)
        };
        let end = std::time::Instant::now();

        let (error, state) = match res {
            Ok(_) => (None, PingState::Ok),
            Err(e) => (Some(e), PingState::Error),
        };

        EndpointPingReport {
            remote: client.remote_addr().to_string(),
            error,
            latency: end.sub(start),
            id: Some(client.id().to_string()),
            namespace: client.bucket_name(),
            state,
        }
    }
}
