use crate::analyticsoptions::AnalyticsOptions;
use crate::analyticsx::analytics::Analytics;
use crate::analyticsx::query_respreader::QueryRespReader;
use crate::analyticsx::query_result::MetaData;
use crate::authenticator::Authenticator;
use crate::error;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager, DEFAULT_RETRY_STRATEGY};
use crate::service_type::ServiceType;
use crate::tracingcomponent::TracingComponent;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct AnalyticsComponent<C: Client> {
    http_component: HttpComponent<C>,
    tracing: Arc<TracingComponent>,

    retry_manager: Arc<RetryManager>,
}

#[derive(Debug)]
pub(crate) struct AnalyticsComponentConfig {
    pub endpoints: HashMap<String, String>,
    pub authenticator: Arc<Authenticator>,
}

pub(crate) struct AnalyticsComponentOptions {
    pub user_agent: String,
}

pub struct AnalyticsResultStream {
    inner: QueryRespReader,
    endpoint: String,
}

impl AnalyticsResultStream {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        self.inner.metadata().map_err(|e| e.into())
    }
}

impl Stream for AnalyticsResultStream {
    type Item = error::Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map_err(|e| e.into())
    }
}

impl<C: Client> AnalyticsComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        tracing: Arc<TracingComponent>,
        config: AnalyticsComponentConfig,
        opts: AnalyticsComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::Analytics,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            tracing,
            retry_manager,
        }
    }

    pub fn reconfigure(&self, config: AnalyticsComponentConfig) {
        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ))
    }

    pub async fn query(&self, opts: AnalyticsOptions<'_>) -> error::Result<AnalyticsResultStream> {
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
                        let res = match (Analytics::<C> {
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
                            Err(e) => return Err(ErrorKind::Analytics(e).into()),
                        };

                        Ok(AnalyticsResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }
}
