use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::{error, queryx};
use crate::authenticator::Authenticator;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::queryoptions::QueryOptions;
use crate::queryx::preparedquery::{PreparedQuery, PreparedStatementCache};
use crate::queryx::query::Query;
use crate::queryx::query_respreader::QueryRespReader;
use crate::queryx::query_result::{EarlyMetaData, MetaData, ResultStream};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager};
use crate::service_type::ServiceType;

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

    pub fn early_metadata(&self) -> Option<&EarlyMetaData> {
        self.inner.early_metadata()
    }

    // TODO: map these errors maybe?
    pub fn metadata(self) -> queryx::error::Result<MetaData> {
        self.inner.metadata()
    }

    pub async fn read_row(&mut self) -> queryx::error::Result<Option<Bytes>> {
        self.inner.read_row().await
    }
}

impl<C: Client> QueryComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
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
        let retry_info = RetryInfo::new(
            opts.read_only.unwrap_or_default(),
            opts.retry_strategy.clone(),
        );

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.orchestrate_query_endpoint(
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
        let retry_info = RetryInfo::new(
            opts.read_only.unwrap_or_default(),
            opts.retry_strategy.clone(),
        );

        let endpoint = opts.endpoint.clone();
        let copts = opts.into();

        orchestrate_retries(self.retry_manager.clone(), retry_info, async || {
            self.orchestrate_query_endpoint(
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

    pub(crate) async fn orchestrate_query_endpoint<Resp, Fut>(
        &self,
        endpoint_id: Option<String>,
        operation: impl Fn(Arc<C>, String, String, String, String) -> Fut + Send + Sync,
    ) -> error::Result<Resp>
    where
        C: Client,
        Fut: Future<Output = error::Result<Resp>> + Send,
        Resp: Send,
    {
        if let Some(endpoint_id) = endpoint_id {
            let (client, endpoint_properties) =
                self.http_component.select_specific_endpoint(&endpoint_id)?;

            return operation(
                client,
                endpoint_id,
                endpoint_properties.endpoint,
                endpoint_properties.username,
                endpoint_properties.password,
            )
            .await;
        }

        let (client, endpoint_properties) =
            if let Some(selected) = self.http_component.select_endpoint(vec![])? {
                selected
            } else {
                return Err(ErrorKind::ServiceNotAvailable {
                    service: ServiceType::Query,
                }
                .into());
            };

        operation(
            client,
            endpoint_properties.endpoint_id.unwrap_or_default(),
            endpoint_properties.endpoint,
            endpoint_properties.username,
            endpoint_properties.password,
        )
        .await
    }
}
