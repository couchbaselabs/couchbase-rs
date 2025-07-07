use crate::authenticator::Authenticator;
use crate::diagnosticscomponent::PingQueryOptions;
use crate::error;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::mgmtx::node_target::NodeTarget;
use crate::pingreport::{EndpointPingReport, PingState};
use crate::queryoptions::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, EnsureIndexOptions, GetAllIndexesOptions, QueryOptions,
    WatchIndexesOptions,
};
use crate::queryx::ensure_index_helper::EnsureIndexHelper;
use crate::queryx::index::Index;
use crate::queryx::preparedquery::{PreparedQuery, PreparedStatementCache};
use crate::queryx::query::Query;
use crate::queryx::query_options::{EnsureIndexPollOptions, PingOptions};
use crate::queryx::query_respreader::QueryRespReader;
use crate::queryx::query_result::{EarlyMetaData, MetaData};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager, DEFAULT_RETRY_STRATEGY};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::service_type::ServiceType;
use bytes::Bytes;
use futures::future::join_all;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Sub;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;

pub(crate) struct QueryComponent<C: Client> {
    http_component: HttpComponent<C>,

    retry_manager: Arc<RetryManager>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
}

#[derive(Debug)]
pub(crate) struct QueryComponentConfig {
    pub endpoints: HashMap<String, String>,
    pub authenticator: Arc<Authenticator>,
}

pub(crate) struct QueryComponentOptions {
    pub user_agent: String,
}

pub struct QueryResultStream {
    inner: QueryRespReader,
    endpoint: String,
}

impl QueryResultStream {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn early_metadata(&self) -> &EarlyMetaData {
        self.inner.early_metadata()
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        self.inner.metadata().map_err(|e| e.into())
    }
}

impl Stream for QueryResultStream {
    type Item = error::Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map_err(|e| e.into())
    }
}

impl<C: Client + 'static> QueryComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        config: QueryComponentConfig,
        opts: QueryComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::QUERY,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            retry_manager,
            prepared_cache: Arc::new(Mutex::new(PreparedStatementCache::default())),
        }
    }

    pub fn reconfigure(&self, config: QueryComponentConfig) {
        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query", opts.read_only.unwrap_or_default(), retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (Query::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                        }
                        .query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Query(e).into()),
                        };

                        Ok(QueryResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> error::Result<QueryResultStream> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };
        let retry_info =
            RetryInfo::new("prepared_query", opts.read_only.unwrap_or_default(), retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (PreparedQuery {
                            executor: Query::<C> {
                                http_client: client,
                                user_agent: self.http_component.user_agent().to_string(),
                                endpoint: endpoint.clone(),
                                username,
                                password,
                            },
                            cache: self.prepared_cache.clone(),
                        }
                        .prepared_query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Query(e).into()),
                        };

                        Ok(QueryResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn get_all_indexes(
        &self,
        opts: &GetAllIndexesOptions<'_>,
    ) -> error::Result<Vec<Index>> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_get_all_indexes", true, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        let res = match (Query::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                        }
                        .get_all_indexes(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => {
                                return Err(ErrorKind::Query(e).into());
                            }
                        };

                        Ok(res)
                    },
                )
                .await
        })
        .await
    }

    pub async fn create_primary_index(
        &self,
        opts: &CreatePrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_create_primary_index", false, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .create_primary_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_create_index", false, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .create_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn drop_primary_index(
        &self,
        opts: &DropPrimaryIndexOptions<'_>,
    ) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_drop_primary_index", false, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .drop_primary_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_drop_index", false, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .drop_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_build_deferred_indexes", false, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .build_deferred_indexes(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("query_watch_indexes", true, retry);

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |query| {
                query
                    .watch_indexes(&copts)
                    .await
                    .map_err(|e| ErrorKind::Query(e).into())
            },
        )
        .await
    }

    pub async fn ensure_index(&self, opts: &EnsureIndexOptions<'_>) -> error::Result<()> {
        let mut helper = EnsureIndexHelper::new(
            self.http_component.user_agent(),
            opts.index_name,
            opts.bucket_name,
            opts.scope_name,
            opts.collection_name,
            opts.on_behalf_of_info,
        );

        let backoff = ExponentialBackoffCalculator::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            1.5,
        );

        self.http_component
            .ensure_resource(backoff, async |client: Arc<C>, targets: Vec<NodeTarget>| {
                helper
                    .clone()
                    .poll(&EnsureIndexPollOptions {
                        client,
                        targets,
                        desired_state: opts.desired_state,
                    })
                    .await
                    .map_err(error::Error::from)
            })
            .await
    }

    pub async fn ping_all_endpoints(
        &self,
        opts: PingQueryOptions<'_>,
    ) -> error::Result<Vec<EndpointPingReport>> {
        let (client, targets) = self.http_component.get_all_targets::<NodeTarget>(&[])?;

        let copts = PingOptions {
            on_behalf_of: opts.on_behalf_of,
        };
        let timeout = opts.timeout;

        let mut handles = Vec::with_capacity(targets.len());
        let user_agent = self.http_component.user_agent().to_string();
        for target in targets {
            let user_agent = user_agent.clone();
            let client = Query::<C> {
                http_client: client.clone(),
                user_agent,
                endpoint: target.endpoint.clone(),
                username: target.username,
                password: target.password,
            };

            let handle = self.ping_one(client, timeout, copts.clone());

            handles.push(handle);
        }

        let reports = join_all(handles).await;

        Ok(reports)
    }

    async fn ping_one(
        &self,
        client: Query<C>,
        timeout: Duration,
        opts: PingOptions<'_>,
    ) -> EndpointPingReport {
        let start = std::time::Instant::now();
        let res = select! {
            e = tokio::time::sleep(timeout) => {
                return EndpointPingReport {
                    remote: client.endpoint,
                    error: None,
                    latency: std::time::Instant::now().sub(start),
                    id: None,
                    namespace: None,
                    state: PingState::Timeout,
                }
            }
            r = client.ping(&opts) => r.map_err(error::Error::from),
        };
        let end = std::time::Instant::now();

        let (error, state) = match res {
            Ok(_) => (None, PingState::Ok),
            Err(e) => (Some(e), PingState::Error),
        };

        EndpointPingReport {
            remote: client.endpoint,
            error,
            latency: end.sub(start),
            id: None,
            namespace: None,
            state,
        }
    }

    async fn orchestrate_no_res_mgmt_call<Fut>(
        &self,
        retry_info: RetryInfo,
        endpoint: Option<String>,
        operation: impl Fn(Query<C>) -> Fut + Send + Sync,
    ) -> error::Result<()>
    where
        Fut: Future<Output = error::Result<()>> + Send,
        C: Client,
    {
        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.http_component
                .orchestrate_endpoint(
                    endpoint.clone(),
                    async |client: Arc<C>,
                           endpoint_id: String,
                           endpoint: String,
                           username: String,
                           password: String| {
                        operation(Query::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,
                        })
                        .await
                    },
                )
                .await
        })
        .await
    }
}
