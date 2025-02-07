use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::queryoptions::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, GetAllIndexesOptions, QueryOptions, WatchIndexesOptions,
};
use crate::queryx::index::Index;
use crate::queryx::preparedquery::{PreparedQuery, PreparedStatementCache};
use crate::queryx::query::Query;
use crate::queryx::query_respreader::QueryRespReader;
use crate::queryx::query_result::{EarlyMetaData, MetaData};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager, DEFAULT_RETRY_STRATEGY};
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use bytes::Bytes;
use futures::{Stream, StreamExt};

pub(crate) struct QueryComponent<C: Client> {
    http_component: HttpComponent<C>,

    tracing: Arc<TracingComponent>,
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

impl<C: Client> QueryComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        tracing: Arc<TracingComponent>,
        config: QueryComponentConfig,
        opts: QueryComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::Query,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            tracing,
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

        let retry_info = RetryInfo::new(opts.read_only.unwrap_or_default(), retry);

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
                            tracing: self.tracing.clone(),
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
        let retry_info = RetryInfo::new(opts.read_only.unwrap_or_default(), retry);

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
                                tracing: self.tracing.clone(),
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

        let retry_info = RetryInfo::new(true, retry);

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
                            tracing: self.tracing.clone(),
                        }
                        .get_all_indexes(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Query(e).into()),
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

        let retry_info = RetryInfo::new(false, retry);

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

        let retry_info = RetryInfo::new(false, retry);

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

        let retry_info = RetryInfo::new(false, retry);

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

        let retry_info = RetryInfo::new(false, retry);

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

        let retry_info = RetryInfo::new(false, retry);

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

        let retry_info = RetryInfo::new(true, retry);

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
                            tracing: self.tracing.clone(),
                        })
                        .await
                    },
                )
                .await
        })
        .await
    }
}
