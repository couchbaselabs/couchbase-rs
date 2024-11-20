use crate::authenticator::Authenticator;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::{
    orchestrate_retries, RetryInfo, RetryManager, RetryStrategy, DEFAULT_RETRY_STRATEGY,
};
use crate::searchoptions::SearchOptions;
use crate::searchx::search::Search;
use crate::searchx::search_respreader::SearchRespReader;
use crate::searchx::search_result::{FacetResult, MetaData, ResultHit};
use crate::service_type::ServiceType;
use crate::{error, searchx};
use arc_swap::ArcSwap;
use futures::StreamExt;
use futures_core::Stream;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use typed_builder::TypedBuilder;

pub(crate) struct SearchComponent<C: Client> {
    http_component: HttpComponent<C>,

    retry_manager: Arc<RetryManager>,

    state: ArcSwap<SearchComponentState>,
}

#[derive(Debug)]
pub(crate) struct SearchComponentState {
    pub vector_search_enabled: bool,
}

#[derive(Debug)]
pub(crate) struct SearchComponentConfig {
    pub endpoints: HashMap<String, String>,
    pub authenticator: Arc<Authenticator>,

    pub vector_search_enabled: bool,
}

#[derive(Debug)]
pub(crate) struct SearchComponentOptions {
    pub user_agent: String,
}

pub struct SearchResultStream {
    inner: SearchRespReader,
    endpoint: String,
}

impl SearchResultStream {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn facets(&self) -> error::Result<&HashMap<String, FacetResult>> {
        self.inner.facets().map_err(|e| e.into())
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        self.inner.metadata().map_err(|e| e.into())
    }
}

impl Stream for SearchResultStream {
    type Item = error::Result<ResultHit>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map_err(|e| e.into())
    }
}
impl<C: Client> SearchComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        config: SearchComponentConfig,
        opts: SearchComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::Search,
                opts.user_agent,
                http_client,
                HttpComponentState::new(config.endpoints, config.authenticator),
            ),
            retry_manager,
            state: ArcSwap::new(Arc::new(SearchComponentState {
                vector_search_enabled: config.vector_search_enabled,
            })),
        }
    }

    pub fn reconfigure(&self, config: SearchComponentConfig) {
        self.http_component.reconfigure(HttpComponentState::new(
            config.endpoints,
            config.authenticator,
        ));

        self.state.swap(Arc::new(SearchComponentState {
            vector_search_enabled: config.vector_search_enabled,
        }));
    }

    pub async fn query(&self, opts: SearchOptions) -> error::Result<SearchResultStream> {
        if (opts.knn.is_some() || opts.knn_operator.is_some())
            && !self.state.load().vector_search_enabled
        {
            return Err(ErrorKind::FeatureNotAvailable {
                feature: "Vector Search".to_string(),
                msg: "vector queries are available from Couchbase Server 7.6.0 and above"
                    .to_string(),
            }
            .into());
        }
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
                        let res = match (Search::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,

                            vector_search_enabled: self.state.load().vector_search_enabled,
                        }
                        .query(&copts)
                        .await)
                        {
                            Ok(r) => r,
                            Err(e) => return Err(ErrorKind::Search(e).into()),
                        };

                        Ok(SearchResultStream {
                            inner: res,
                            endpoint,
                        })
                    },
                )
                .await
        })
        .await
    }

    pub async fn upsert_index<'a>(&self, opts: &UpsertIndexOptions<'a>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new(true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .upsert_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn delete_index<'a>(&self, opts: &DeleteIndexOptions<'a>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new(true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .delete_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    async fn orchestrate_no_res_mgmt_call<Fut>(
        &self,
        mut retry_info: RetryInfo,
        endpoint: Option<String>,
        operation: impl Fn(Search<C>) -> Fut + Send + Sync,
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
                        operation(Search::<C> {
                            http_client: client,
                            user_agent: self.http_component.user_agent().to_string(),
                            endpoint: endpoint.clone(),
                            username,
                            password,

                            vector_search_enabled: self.state.load().vector_search_enabled,
                        })
                        .await
                    },
                )
                .await
        })
        .await
    }
}

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct UpsertIndexOptions<'a> {
    #[builder(!default)]
    pub index: &'a searchx::index::Index,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,

    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub endpoint: Option<&'a str>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

#[derive(Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct DeleteIndexOptions<'a> {
    #[builder(!default)]
    pub index_name: &'a str,
    pub bucket_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,

    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub endpoint: Option<&'a str>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> From<&UpsertIndexOptions<'a>> for searchx::search::UpsertIndexOptions<'a> {
    fn from(opts: &UpsertIndexOptions<'a>) -> searchx::search::UpsertIndexOptions<'a> {
        searchx::search::UpsertIndexOptions {
            index: opts.index,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> From<&DeleteIndexOptions<'a>> for searchx::search::DeleteIndexOptions<'a> {
    fn from(opts: &DeleteIndexOptions<'a>) -> searchx::search::DeleteIndexOptions<'a> {
        searchx::search::DeleteIndexOptions {
            index_name: opts.index_name,
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
