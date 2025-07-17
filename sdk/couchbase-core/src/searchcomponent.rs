use crate::authenticator::Authenticator;
use crate::diagnosticscomponent::PingSearchReportOptions;
use crate::error::ErrorKind;
use crate::httpcomponent::{HttpComponent, HttpComponentState};
use crate::httpx::client::Client;
use crate::mgmtx::node_target::NodeTarget;
use crate::options::search::SearchOptions;
use crate::options::search_management::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    EnsureIndexOptions, FreezePlanOptions, GetAllIndexesOptions, GetIndexOptions,
    GetIndexedDocumentsCountOptions, PauseIngestOptions, ResumeIngestOptions, UnfreezePlanOptions,
    UpsertIndexOptions,
};
use crate::pingreport::{EndpointPingReport, PingState};
use crate::retry::{orchestrate_retries, RetryInfo, RetryManager, DEFAULT_RETRY_STRATEGY};
use crate::retrybesteffort::ExponentialBackoffCalculator;
use crate::searchx::document_analysis::DocumentAnalysis;
use crate::searchx::ensure_index_helper::EnsureIndexHelper;
use crate::searchx::index::Index;
use crate::searchx::mgmt_options::{EnsureIndexPollOptions, PingOptions};
use crate::searchx::search::Search;
use crate::searchx::search_respreader::SearchRespReader;
use crate::searchx::search_result::{FacetResult, MetaData, ResultHit};
use crate::service_type::ServiceType;
use crate::{error, httpx};
use arc_swap::ArcSwap;
use futures::future::join_all;
use futures::StreamExt;
use futures_core::Stream;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;

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
impl<C: Client + 'static> SearchComponent<C> {
    pub fn new(
        retry_manager: Arc<RetryManager>,
        http_client: Arc<C>,
        config: SearchComponentConfig,
        opts: SearchComponentOptions,
    ) -> Self {
        Self {
            http_component: HttpComponent::new(
                ServiceType::SEARCH,
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

        let retry_info = RetryInfo::new("search_query", true, retry);

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

    pub async fn get_index(&self, opts: &GetIndexOptions<'_>) -> error::Result<Index> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_get_index", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .get_index(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
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

        let retry_info = RetryInfo::new("search_get_all_indexes", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .get_all_indexes(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn upsert_index(&self, opts: &UpsertIndexOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_upsert_index", true, retry);
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

    pub async fn delete_index(&self, opts: &DeleteIndexOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_delete_index", true, retry);
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

    pub async fn analyze_document(
        &self,
        opts: &AnalyzeDocumentOptions<'_>,
    ) -> error::Result<DocumentAnalysis> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_analyze_document", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .analyze_document(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn get_indexed_documents_count(
        &self,
        opts: &GetIndexedDocumentsCountOptions<'_>,
    ) -> error::Result<u64> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_get_indexed_documents_count", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .get_indexed_documents_count(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn pause_ingest(&self, opts: &PauseIngestOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_pause_ingest", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .pause_ingest(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn resume_ingest(&self, opts: &ResumeIngestOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_resume_ingest", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .resume_ingest(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn allow_querying(&self, opts: &AllowQueryingOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_allow_querying", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .allow_querying(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn disallow_querying(&self, opts: &DisallowQueryingOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_disallow_querying", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .disallow_querying(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn freeze_plan(&self, opts: &FreezePlanOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_freeze_plan", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .freeze_plan(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
            },
        )
        .await
    }

    pub async fn unfreeze_plan(&self, opts: &UnfreezePlanOptions<'_>) -> error::Result<()> {
        let retry = if let Some(retry_strategy) = opts.retry_strategy.clone() {
            retry_strategy
        } else {
            DEFAULT_RETRY_STRATEGY.clone()
        };

        let retry_info = RetryInfo::new("search_unfreeze_plan", true, retry);
        let endpoint = opts.endpoint;
        let copts = opts.into();

        self.orchestrate_no_res_mgmt_call(
            retry_info,
            endpoint.map(|e| e.to_string()),
            async |search| {
                search
                    .unfreeze_plan(&copts)
                    .await
                    .map_err(|e| ErrorKind::Search(e).into())
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
        on_behalf_of: Option<&httpx::request::OnBehalfOfInfo>,
    ) -> error::Result<Vec<error::Result<()>>> {
        let (client, targets) = self.http_component.get_all_targets::<NodeTarget>(&[])?;

        let copts = PingOptions { on_behalf_of };

        let mut handles = Vec::with_capacity(targets.len());
        let user_agent = self.http_component.user_agent().to_string();
        for target in targets {
            let user_agent = user_agent.clone();
            let client = Search::<C> {
                http_client: client.clone(),
                user_agent,
                endpoint: target.endpoint.clone(),
                username: target.username,
                password: target.password,
                vector_search_enabled: false,
            };

            let handle = self.ping_one(client, copts.clone());

            handles.push(handle);
        }

        let results = join_all(handles).await;

        Ok(results)
    }

    pub async fn create_ping_report(
        &self,
        opts: PingSearchReportOptions<'_>,
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
            let client = Search::<C> {
                http_client: client.clone(),
                user_agent,
                endpoint: target.endpoint.clone(),
                username: target.username,
                password: target.password,

                vector_search_enabled: self.state.load().vector_search_enabled,
            };

            let handle = self.create_one_report(client, timeout, copts.clone());

            handles.push(handle);
        }

        let reports = join_all(handles).await;

        Ok(reports)
    }

    async fn ping_one(&self, client: Search<C>, opts: PingOptions<'_>) -> error::Result<()> {
        client.ping(&opts).await.map_err(error::Error::from)
    }

    async fn create_one_report(
        &self,
        client: Search<C>,
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

    async fn orchestrate_mgmt_call<Fut, Resp>(
        &self,
        retry_info: RetryInfo,
        endpoint: Option<String>,
        operation: impl Fn(Search<C>) -> Fut + Send + Sync,
    ) -> error::Result<Resp>
    where
        Resp: Send + Sync,
        Fut: Future<Output = error::Result<Resp>> + Send,
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

    async fn orchestrate_no_res_mgmt_call<Fut>(
        &self,
        retry_info: RetryInfo,
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
