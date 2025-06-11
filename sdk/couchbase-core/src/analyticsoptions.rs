use crate::analyticsx;
use crate::analyticsx::query_options::ScanConsistency;
use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::RetryStrategy;
use serde_json::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AnalyticsOptions<'a> {
    pub client_context_id: Option<&'a str>,
    pub priority: Option<i32>,
    pub query_context: Option<&'a str>,
    pub read_only: Option<bool>,
    pub scan_consistency: Option<ScanConsistency>,
    pub statement: &'a str,

    pub args: Option<&'a [Value]>,
    pub named_args: Option<&'a HashMap<String, Value>>,
    pub raw: Option<&'a HashMap<String, Value>>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl<'a> From<AnalyticsOptions<'a>> for analyticsx::query_options::QueryOptions<'a> {
    fn from(opts: AnalyticsOptions<'a>) -> Self {
        Self {
            client_context_id: opts.client_context_id,
            priority: opts.priority,
            query_context: opts.query_context,
            read_only: opts.read_only,
            scan_consistency: opts.scan_consistency,
            statement: opts.statement,
            args: opts.args,
            named_args: opts.named_args,
            raw: opts.raw,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

impl<'a> AnalyticsOptions<'a> {
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
            endpoint: None,
            retry_strategy: None,
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

    pub fn endpoint(mut self, endpoint: impl Into<Option<String>>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub fn retry_strategy(
        mut self,
        retry_strategy: impl Into<Option<Arc<dyn RetryStrategy>>>,
    ) -> Self {
        self.retry_strategy = retry_strategy.into();
        self
    }
}
