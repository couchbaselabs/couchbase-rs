use crate::analyticsx;
use crate::analyticsx::query_options::ScanConsistency;
use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::RetryStrategy;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct AnalyticsOptions<'a> {
    pub client_context_id: Option<&'a str>,
    pub priority: Option<i32>,
    pub query_context: Option<&'a str>,
    pub read_only: Option<bool>,
    pub scan_consistency: Option<ScanConsistency>,
    #[builder(!default)]
    pub statement: &'a str,

    pub args: Option<&'a [&'a RawValue]>,
    pub named_args: Option<&'a HashMap<&'a str, &'a RawValue>>,
    pub raw: Option<&'a HashMap<&'a str, &'a RawValue>>,

    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl<'a> From<&AnalyticsOptions<'a>> for analyticsx::query_options::QueryOptions<'a> {
    fn from(opts: &AnalyticsOptions<'a>) -> Self {
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
