use crate::configmanager::ConfigManager;
use crate::error::Error;
use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::kvclient::KvClient;
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::KvClientManager;
use crate::kvclientpool::KvClientPool;
use crate::memdx::request::PingRequest;
use crate::options::ping::PingOptions;
use crate::pingreport::{EndpointPingReport, PingReport, PingState};
use crate::querycomponent::QueryComponent;
use crate::retry::RetryManager;
use crate::searchcomponent::SearchComponent;
use crate::service_type::ServiceType;
use futures::future::join_all;
use serde::ser::SerializeStruct;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Sub;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub(crate) struct PingQueryOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub(crate) timeout: Duration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PingSearchOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub(crate) timeout: Duration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PingKvOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a str>,
    pub(crate) timeout: Duration,
}

pub struct DiagnosticsComponent<C: Client, M: KvClientManager, CM: ConfigManager> {
    kv_client_manager: Arc<M>,
    query_component: Arc<QueryComponent<C>>,
    search_component: Arc<SearchComponent<C>>,
    config_manager: Arc<CM>,

    state: Mutex<DiagnosticsComponentState>,

    retry_manager: Arc<RetryManager>,
}

#[derive(Debug)]
pub(crate) struct DiagnosticsComponentConfig {
    pub bucket: Option<String>,
}

struct DiagnosticsComponentState {
    bucket: Option<String>,
}

impl<C: Client + 'static, M: KvClientManager, CM: ConfigManager> DiagnosticsComponent<C, M, CM> {
    pub fn new(
        kv_client_manager: Arc<M>,
        query_component: Arc<QueryComponent<C>>,
        search_component: Arc<SearchComponent<C>>,
        config_manager: Arc<CM>,
        retry_manager: Arc<RetryManager>,
        config: DiagnosticsComponentConfig,
    ) -> Self {
        Self {
            kv_client_manager,
            query_component,
            search_component,
            config_manager,

            retry_manager,
            state: Mutex::new(DiagnosticsComponentState {
                bucket: config.bucket,
            }),
        }
    }

    pub fn reconfigure(&self, config: DiagnosticsComponentConfig) {
        let mut state_guard = self.state.lock().unwrap();
        state_guard.bucket = config.bucket;
    }

    pub async fn ping(&self, opts: &PingOptions) -> crate::error::Result<PingReport>
    where
        <<M as KvClientManager>::Pool as KvClientPool>::Client: 'static,
    {
        let service_types = if opts.service_types.is_empty() {
            vec![ServiceType::QUERY, ServiceType::SEARCH, ServiceType::MEMD]
        } else {
            opts.service_types.clone()
        };

        let on_behalf_of = opts.on_behalf_of.as_ref();

        let mut services = HashMap::new();
        if service_types.contains(&ServiceType::QUERY) {
            let query_report = self
                .query_component
                .ping_all_endpoints(PingQueryOptions {
                    on_behalf_of,
                    timeout: opts.query_timeout,
                })
                .await?;
            services.insert(ServiceType::QUERY, query_report);
        }

        if service_types.contains(&ServiceType::SEARCH) {
            let search_report = self
                .search_component
                .ping_all_endpoints(PingSearchOptions {
                    on_behalf_of,
                    timeout: opts.search_timeout,
                })
                .await?;
            services.insert(ServiceType::SEARCH, search_report);
        }

        if service_types.contains(&ServiceType::MEMD) {
            let on_behalf_of = on_behalf_of.map(|b| b.username.as_str());
            let kv_report = self
                .ping_all_kv_nodes(PingKvOptions {
                    on_behalf_of,
                    timeout: opts.kv_timeout,
                })
                .await?;
            services.insert(ServiceType::MEMD, kv_report);
        }

        Ok(PingReport {
            version: 2,
            id: Uuid::new_v4().to_string(),
            sdk: "rust".to_string(),
            config_rev: self.config_manager.current_config().rev_id,
            services,
        })
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
        let bucket = {
            let state_guard = self.state.lock().unwrap();
            state_guard.bucket.clone()
        };

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
