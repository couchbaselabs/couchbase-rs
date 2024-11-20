use crate::httpx::request::OnBehalfOfInfo;
use crate::searchx::facets::Facet;
use crate::searchx::queries::Query;
use crate::searchx::sort::Sort;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct Location {
    pub lat: f64,
    pub lon: f64,
}

impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.lon)?;
        seq.serialize_element(&self.lat)?;

        seq.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum HighlightStyle {
    Html,
    Ansi,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum ConsistencyLevel {
    #[serde(rename = "")]
    NotBounded,
    #[serde(rename = "at_plus")]
    AtPlus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ConsistencyResults {
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Highlight {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<HighlightStyle>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
}

pub type ConsistencyVectors = HashMap<String, HashMap<String, u64>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Consistency {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<ConsistencyLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<ConsistencyResults>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vectors: Option<ConsistencyVectors>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Control {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct KnnQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<f32>,
    #[builder(!default)]
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_base64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum KnnOperator {
    Or,
    And,
}

#[derive(Debug, Clone, PartialEq, Serialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct QueryOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "ctl")]
    pub control: Option<Control>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<HashMap<String, Facet>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight: Option<Highlight>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "includeLocations")]
    pub include_locations: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<Query>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_after: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_before: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "showrequest")]
    pub show_request: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Sort>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub knn: Option<Vec<KnnQuery>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub knn_operator: Option<KnnOperator>,

    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub raw: Option<HashMap<String, serde_json::Value>>,

    #[builder(setter(!strip_option))]
    #[serde(skip_serializing)]
    pub index_name: String,
    #[serde(skip_serializing)]
    pub scope_name: Option<String>,
    #[serde(skip_serializing)]
    pub bucket_name: Option<String>,

    #[serde(skip_serializing)]
    pub on_behalf_of: Option<OnBehalfOfInfo>,
}
