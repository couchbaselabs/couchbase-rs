use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::options::search_options::SearchOptions;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use couchbase_core::retry::RetryStrategy;
use couchbase_core::searchx;
use couchbase_core::searchx::query_options::{
    Consistency, ConsistencyLevel, ConsistencyVectors, Control, KnnOperator, KnnQuery,
};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct SearchClient {
    backend: SearchClientBackend,
}

impl SearchClient {
    pub fn new(backend: SearchClientBackend) -> Self {
        Self { backend }
    }

    pub async fn search(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: Option<SearchOptions>,
    ) -> error::Result<SearchResult> {
        match &self.backend {
            SearchClientBackend::CouchbaseSearchClientBackend(backend) => {
                backend.search(index_name, request, opts).await
            }
            SearchClientBackend::Couchbase2SearchClientBackend(backend) => {
                backend.search(index_name, request, opts).await
            }
        }
    }
}

pub(crate) enum SearchClientBackend {
    CouchbaseSearchClientBackend(CouchbaseSearchClient),
    Couchbase2SearchClientBackend(Couchbase2SearchClient),
}

pub(crate) struct SearchKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

pub(crate) struct CouchbaseSearchClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: Option<SearchKeyspace>,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseSearchClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            keyspace: None,
            default_retry_strategy,
        }
    }

    pub fn with_keyspace(
        agent_provider: CouchbaseAgentProvider,
        keyspace: SearchKeyspace,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            keyspace: Some(keyspace),
            default_retry_strategy,
        }
    }

    pub async fn search(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: Option<SearchOptions>,
    ) -> error::Result<SearchResult> {
        let opts = opts.unwrap_or_default();

        let score = if let Some(disable_scoring) = opts.disable_scoring {
            if disable_scoring {
                Some("none".to_string())
            } else {
                None
            }
        } else {
            None
        };

        if opts.consistent_with.is_some() && opts.scan_consistency.is_some() {
            return Err(error::Error::invalid_argument(
                "consistent_with",
                "cannot be used with scan_consistency",
            ));
        }

        let control = {
            let scan_consistency = if let Some(scan_consistency) = opts.scan_consistency {
                Some(Consistency::default().level(scan_consistency))
            } else if let Some(consistent_with) = opts.consistent_with {
                let mut vectors: ConsistencyVectors = HashMap::default();
                for token in consistent_with.tokens() {
                    let vector = vectors.entry(index_name.clone()).or_default();
                    vector.insert(
                        format!("{}/{}", token.token.vbid(), token.token.vbuuid()),
                        token.token.seqno(),
                    );
                }

                Some(
                    Consistency::default()
                        .level(ConsistencyLevel::AtPlus)
                        .vectors(vectors),
                )
            } else {
                None
            };

            if scan_consistency.is_some() || opts.server_timeout.is_some() {
                Some(
                    Control::default()
                        .consistency(scan_consistency)
                        .timeout(opts.server_timeout.map(|t| t.as_millis() as u64)),
                )
            } else {
                None
            }
        };

        let (knn, knn_operator) = if let Some(vector_search) = request.vector_search {
            let queries: Vec<KnnQuery> = vector_search
                .vector_queries
                .into_iter()
                .map(KnnQuery::try_from)
                .collect::<error::Result<Vec<KnnQuery>>>()?;
            let operator: Option<KnnOperator> = vector_search.query_combination.map(|qc| qc.into());

            (Some(queries), operator)
        } else {
            (None, None)
        };

        let (bucket_name, scope_name) = if let Some(keyspace) = &self.keyspace {
            (
                Some(keyspace.bucket_name.clone()),
                Some(keyspace.scope_name.clone()),
            )
        } else {
            (None, None)
        };

        let query = if let Some(query) = request.search_query {
            Some(query.into())
        } else {
            Some(searchx::queries::Query::MatchNone(
                searchx::queries::MatchNoneQuery::default(),
            ))
        };

        let facets = if let Some(facets) = opts.facets {
            let mut core_facets = HashMap::with_capacity(facets.len());
            for (name, facet) in facets {
                core_facets.insert(name, facet.into());
            }

            Some(core_facets)
        } else {
            None
        };

        let core_opts = couchbase_core::options::search::SearchOptions::new(index_name)
            .collections(opts.collections)
            .control(control)
            .explain(opts.explain)
            .facets(facets)
            .fields(opts.fields)
            .from(opts.skip)
            .highlight(opts.highlight.map(|h| h.into()))
            .include_locations(opts.include_locations)
            .query(query)
            .score(score)
            .search_after(None)
            .search_before(None)
            .show_request(false)
            .size(opts.limit)
            .sort(opts.sort.map(|s| s.into_iter().map(|i| i.into()).collect()))
            .knn(knn)
            .knn_operator(knn_operator)
            .raw(opts.raw)
            .scope_name(scope_name)
            .bucket_name(bucket_name)
            .on_behalf_of(None)
            .endpoint(None)
            .retry_strategy(self.default_retry_strategy.clone());

        let agent = self.agent_provider.get_agent().await;
        Ok(SearchResult::from(agent.search(core_opts).await?))
    }
}

pub(crate) struct Couchbase2SearchClient {}

impl Couchbase2SearchClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn search(
        &self,
        _index_name: String,
        _request: SearchRequest,
        _opts: Option<SearchOptions>,
    ) -> error::Result<SearchResult> {
        unimplemented!()
    }
}
