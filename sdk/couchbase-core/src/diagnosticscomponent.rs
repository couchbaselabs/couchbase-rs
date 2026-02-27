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

use crate::configmanager::ConfigManager;
use crate::connection_state::ConnectionState;
use crate::error::Error;
use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientpool::KvClientPool;
use crate::kvendpointclientmanager::KvEndpointClientManager;
use crate::memdx::request::PingRequest;
use crate::options::diagnostics::DiagnosticsOptions;
use crate::options::ping::PingOptions;
use crate::options::waituntilready::{ClusterState, WaitUntilReadyOptions};
use crate::querycomponent::QueryComponent;
use crate::results::diagnostics::{DiagnosticsResult, EndpointDiagnostics};
use crate::results::pingreport::{EndpointPingReport, PingReport, PingState};
use crate::retry::{RetryManager, RetryReason, RetryRequest};
use crate::retrybesteffort::{BestEffortRetryStrategy, ExponentialBackoffCalculator};
use crate::searchcomponent::SearchComponent;
use crate::service_type::ServiceType;
use chrono::Utc;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use futures_core::future::BoxFuture;
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
use tracing::debug;
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

pub struct DiagnosticsComponent<C: Client, M: KvEndpointClientManager> {
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

impl<C: Client + 'static, M: KvEndpointClientManager> DiagnosticsComponent<C, M> {
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
        let endpoint_reports = self.kv_client_manager.endpoint_diagnostics().await;

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
        <M as KvEndpointClientManager>::Client: 'static,
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

        let mut retry_info = RetryRequest::new("wait_until_ready", true);

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
                .maybe_retry(
                    opts.retry_strategy.clone(),
                    &mut retry_info,
                    RetryReason::NotReady,
                )
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
        let req = PingRequest { on_behalf_of };

        let responses = self.kv_client_manager.ping_all_clients(req).await;

        let mut pools_ok = Vec::with_capacity(responses.len());
        for response in responses.values() {
            let is_pool_ok = if desired_state == ClusterState::Online {
                response.iter().all(|res| res.is_ok())
            } else {
                response.iter().any(|res| res.is_ok())
            };

            pools_ok.push(is_pool_ok);
        }

        pools_ok.into_iter().all(|ready| ready)
    }

    async fn ping_all_kv_nodes(
        &self,
        opts: PingKvOptions<'_>,
    ) -> crate::error::Result<Vec<EndpointPingReport>>
    where
        <M as KvEndpointClientManager>::Client: 'static,
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
        client: Arc<<M as KvEndpointClientManager>::Client>,
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
