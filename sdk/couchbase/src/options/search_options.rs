use crate::error::Error;
use crate::mutation_state::MutationState;
use crate::search::facets::Facet;
use crate::search::sort::Sort;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Highlight {
    pub(crate) style: Option<HighlightStyle>,
    pub(crate) fields: Option<Vec<String>>,
}

impl Highlight {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn style(mut self, style: HighlightStyle) -> Self {
        self.style = Some(style);
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }
}
impl From<Highlight> for couchbase_core::searchx::query_options::Highlight {
    fn from(h: Highlight) -> Self {
        couchbase_core::searchx::query_options::Highlight::default()
            .style(h.style.map(|s| s.into()))
            .fields(h.fields)
    }
}

impl From<Highlight> for Option<couchbase_core::searchx::query_options::Highlight> {
    fn from(h: Highlight) -> Self {
        Some(h.into())
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct SearchOptions {
    pub(crate) collections: Option<Vec<String>>,
    pub(crate) limit: Option<u32>,
    pub(crate) skip: Option<u32>,
    pub(crate) explain: Option<bool>,
    pub(crate) highlight: Option<Highlight>,
    pub(crate) fields: Option<Vec<String>>,
    pub(crate) scan_consistency: Option<ScanConsistency>,
    pub(crate) consistent_with: Option<MutationState>,
    pub(crate) sort: Option<Vec<Sort>>,
    pub(crate) facets: Option<HashMap<String, Facet>>,
    pub(crate) raw: Option<HashMap<String, Value>>,
    pub(crate) include_locations: Option<bool>,
    pub(crate) disable_scoring: Option<bool>,
    pub(crate) server_timeout: Option<Duration>,
}

impl SearchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn collections(mut self, collections: Vec<String>) -> Self {
        self.collections = Some(collections);
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }

    pub fn explain(mut self, explain: bool) -> Self {
        self.explain = Some(explain);
        self
    }

    pub fn highlight(mut self, highlight: Highlight) -> Self {
        self.highlight = Some(highlight);
        self
    }

    pub fn fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn consistent_with(mut self, consistent_with: MutationState) -> Self {
        self.consistent_with = Some(consistent_with);
        self
    }

    pub fn sort(mut self, sort: Vec<Sort>) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn facets(mut self, facets: HashMap<String, Facet>) -> Self {
        self.facets = Some(facets);
        self
    }

    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> crate::error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.raw {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
        }
        Ok(self)
    }

    pub fn include_locations(mut self, include_locations: bool) -> Self {
        self.include_locations = Some(include_locations);
        self
    }

    pub fn disable_scoring(mut self, disable_scoring: bool) -> Self {
        self.disable_scoring = Some(disable_scoring);
        self
    }

    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }
}
