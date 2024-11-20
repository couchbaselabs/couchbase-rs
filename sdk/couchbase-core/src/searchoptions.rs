use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::RetryStrategy;
use crate::searchx;
use crate::searchx::facets::Facet;
use crate::searchx::queries::Query;
use crate::searchx::query_options::{Control, Highlight, KnnOperator, KnnQuery};
use crate::searchx::sort::Sort;
use std::collections::HashMap;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct SearchOptions {
    pub collections: Option<Vec<String>>,
    pub control: Option<Control>,
    pub explain: Option<bool>,
    pub facets: Option<HashMap<String, Facet>>,
    pub fields: Option<Vec<String>>,
    pub from: Option<u32>,
    pub highlight: Option<Highlight>,
    pub include_locations: Option<bool>,
    pub query: Option<Query>,
    pub score: Option<String>,
    pub search_after: Option<Vec<String>>,
    pub search_before: Option<Vec<String>>,
    pub show_request: Option<bool>,
    pub size: Option<u32>,
    pub sort: Option<Vec<Sort>>,
    pub knn: Option<Vec<KnnQuery>>,
    pub knn_operator: Option<KnnOperator>,

    pub raw: Option<HashMap<String, serde_json::Value>>,

    pub index_name: String,
    pub scope_name: Option<String>,
    pub bucket_name: Option<String>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,

    pub endpoint: Option<String>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl From<SearchOptions> for searchx::query_options::QueryOptions {
    fn from(opts: SearchOptions) -> Self {
        searchx::query_options::QueryOptions {
            collections: opts.collections,
            control: opts.control,
            explain: opts.explain,
            facets: opts.facets,
            fields: opts.fields,
            from: opts.from,
            highlight: opts.highlight,
            include_locations: opts.include_locations,
            query: opts.query,
            score: opts.score,
            search_after: opts.search_after,
            search_before: opts.search_before,
            show_request: opts.show_request,
            size: opts.size,
            sort: opts.sort,
            knn: opts.knn,
            knn_operator: opts.knn_operator,
            raw: opts.raw,
            index_name: opts.index_name,
            scope_name: opts.scope_name,
            bucket_name: opts.bucket_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
