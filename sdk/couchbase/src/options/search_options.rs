use crate::mutation_state::MutationState;
use crate::search::facets::Facet;
use crate::search::sort::Sort;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
}

impl From<ScanConsistency> for couchbase_core::searchx::query_options::ConsistencyLevel {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => {
                couchbase_core::searchx::query_options::ConsistencyLevel::NotBounded
            }
        }
    }
}

impl From<ScanConsistency> for Option<couchbase_core::searchx::query_options::ConsistencyLevel> {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => {
                Some(couchbase_core::searchx::query_options::ConsistencyLevel::NotBounded)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HighlightStyle {
    Html,
    Ansi,
}

impl From<HighlightStyle> for couchbase_core::searchx::query_options::HighlightStyle {
    fn from(hs: HighlightStyle) -> Self {
        match hs {
            HighlightStyle::Html => couchbase_core::searchx::query_options::HighlightStyle::Html,
            HighlightStyle::Ansi => couchbase_core::searchx::query_options::HighlightStyle::Ansi,
        }
    }
}

impl From<HighlightStyle> for Option<couchbase_core::searchx::query_options::HighlightStyle> {
    fn from(hs: HighlightStyle) -> Self {
        Some(hs.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct Highlight {
    pub style: Option<HighlightStyle>,
    pub fields: Option<Vec<String>>,
}

impl From<Highlight> for couchbase_core::searchx::query_options::Highlight {
    fn from(h: Highlight) -> Self {
        couchbase_core::searchx::query_options::Highlight::builder()
            .style(h.style.map(|s| s.into()))
            .fields(h.fields)
            .build()
    }
}

impl From<Highlight> for Option<couchbase_core::searchx::query_options::Highlight> {
    fn from(h: Highlight) -> Self {
        Some(h.into())
    }
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct SearchOptions {
    pub collections: Option<Vec<String>>,
    pub limit: Option<u32>,
    pub skip: Option<u32>,
    pub explain: Option<bool>,
    pub highlight: Option<Highlight>,
    pub fields: Option<Vec<String>>,
    pub scan_consistency: Option<ScanConsistency>,
    pub consistent_with: Option<MutationState>,
    pub sort: Option<Vec<Sort>>,
    pub facets: Option<HashMap<String, Facet>>,
    pub raw: Option<HashMap<String, Value>>,
    pub include_locations: Option<bool>,
    pub disable_scoring: Option<bool>,
    pub server_timeout: Option<Duration>,

    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}
