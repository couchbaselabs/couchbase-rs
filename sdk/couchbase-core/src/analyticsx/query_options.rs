use crate::httpx::request::OnBehalfOfInfo;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
}

#[derive(Debug, Clone, Default, Serialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct QueryOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_context_id: Option<&'a str>,
    #[serde(skip_serializing)]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_context: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "readonly")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan_consistency: Option<ScanConsistency>,
    #[builder(!default)]
    pub statement: &'a str,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<&'a [&'a RawValue]>,
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub named_args: Option<&'a HashMap<&'a str, &'a RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub raw: Option<&'a HashMap<&'a str, &'a RawValue>>,

    #[serde(skip_serializing)]
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}
