use crate::api::collection::MutationState;
use crate::{CouchbaseError, CouchbaseResult, ErrorContext, SearchFacet, SearchSort};
use serde_derive::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub enum SearchHighlightStyle {
    #[serde(rename = "html")]
    HTML,
    #[serde(rename = "ansi")]
    ANSI,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchHighLight {
    #[serde(rename = "style")]
    #[serde(skip_serializing_if = "Option::is_none")]
    style: Option<SearchHighlightStyle>,
    #[serde(rename = "fields")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum SearchScanConsistency {
    NotBounded,
}

// No idea why it won't let me do this as derive
impl Display for SearchScanConsistency {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            SearchScanConsistency::NotBounded => write!(f, "not_bounded"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchCtlConsistency {
    level: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    vectors: HashMap<String, HashMap<String, u64>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SearchCtl {
    ctl: SearchCtlConsistency,
}

#[derive(Debug, Default, Serialize)]
pub struct SearchOptions {
    #[serde(rename = "size")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit: Option<u32>,
    #[serde(rename = "from")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) skip: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) explain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) highlight: Option<SearchHighLight>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consistency: Option<SearchCtl>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facets: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The query and index are not part of the public API, but added here
    // as a convenience so we can convert the whole block into the
    // JSON payload the search engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    #[serde(rename = "indexName")]
    pub(crate) index: Option<String>,
    pub(crate) query: Option<serde_json::Value>,
}

impl SearchOptions {
    timeout!();

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

    pub fn highlight(mut self, style: Option<SearchHighlightStyle>, fields: Vec<String>) -> Self {
        self.highlight = Some(SearchHighLight { style, fields });
        self
    }

    pub fn fields<I, T>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    pub fn scan_consistency(mut self, level: SearchScanConsistency) -> Self {
        self.consistency = Some(SearchCtl {
            ctl: SearchCtlConsistency {
                level: level.to_string(),
                vectors: HashMap::new(),
            },
        });
        self
    }

    pub fn consistent_with(mut self, state: MutationState) -> Self {
        let mut vectors = HashMap::new();
        for token in state.tokens.into_iter().next() {
            let bucket = token.bucket_name().to_string();
            let vector = vectors.entry(bucket).or_insert(HashMap::new());

            vector.insert(
                format!("{}/{}", token.partition_uuid(), token.partition_id()),
                token.sequence_number(),
            );
        }
        self.consistency = Some(SearchCtl {
            ctl: SearchCtlConsistency {
                level: "at_plus".into(),
                vectors,
            },
        });
        self
    }

    pub fn sort<T>(mut self, sort: Vec<T>) -> CouchbaseResult<Self>
    where
        T: SearchSort,
    {
        let jsonified =
            serde_json::to_value(sort).map_err(CouchbaseError::encoding_failure_from_serde)?;
        self.sort = Some(jsonified);
        Ok(self)
    }

    pub fn facets<T>(mut self, facets: HashMap<String, T>) -> CouchbaseResult<Self>
    where
        T: SearchFacet,
    {
        let jsonified =
            serde_json::to_value(facets).map_err(CouchbaseError::encoding_failure_from_serde)?;
        self.facets = Some(jsonified);
        Ok(self)
    }

    pub fn raw<T>(mut self, raw: T) -> CouchbaseResult<Self>
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => Ok(a),
            Ok(_) => Err(CouchbaseError::InvalidArgument {
                ctx: ErrorContext::from(("raw", "Only objects are allowed")),
            }),
            Err(e) => Err(CouchbaseError::encoding_failure_from_serde(e)),
        }?;
        self.raw = Some(raw);
        Ok(self)
    }
}
