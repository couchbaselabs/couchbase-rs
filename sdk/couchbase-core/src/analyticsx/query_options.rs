use crate::httpx::request::OnBehalfOfInfo;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
}

#[derive(Debug, Clone, Serialize)]
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
    pub statement: &'a str,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<&'a [Value]>,
    #[serde(skip_serializing)]
    pub named_args: Option<&'a HashMap<String, Value>>,
    #[serde(skip_serializing)]
    pub raw: Option<&'a HashMap<String, Value>>,

    #[serde(skip_serializing)]
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> QueryOptions<'a> {
    pub fn new(statement: &'a str) -> Self {
        Self {
            client_context_id: None,
            priority: None,
            query_context: None,
            read_only: None,
            scan_consistency: None,
            statement,

            args: None,
            named_args: None,
            raw: None,
            on_behalf_of: None,
        }
    }

    pub fn client_context_id(mut self, client_context_id: impl Into<Option<&'a str>>) -> Self {
        self.client_context_id = client_context_id.into();
        self
    }

    pub fn priority(mut self, priority: impl Into<Option<i32>>) -> Self {
        self.priority = priority.into();
        self
    }

    pub fn query_context(mut self, query_context: impl Into<Option<&'a str>>) -> Self {
        self.query_context = query_context.into();
        self
    }

    pub fn read_only(mut self, read_only: impl Into<Option<bool>>) -> Self {
        self.read_only = read_only.into();
        self
    }

    pub fn scan_consistency(
        mut self,
        scan_consistency: impl Into<Option<ScanConsistency>>,
    ) -> Self {
        self.scan_consistency = scan_consistency.into();
        self
    }

    pub fn args(mut self, args: impl Into<Option<&'a [Value]>>) -> Self {
        self.args = args.into();
        self
    }

    pub fn named_args(mut self, named_args: impl Into<Option<&'a HashMap<String, Value>>>) -> Self {
        self.named_args = named_args.into();
        self
    }

    pub fn raw(mut self, raw: impl Into<Option<&'a HashMap<String, Value>>>) -> Self {
        self.raw = raw.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}
