use couchbase_core::analyticsx;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
}

impl From<ScanConsistency> for analyticsx::query_options::ScanConsistency {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => analyticsx::query_options::ScanConsistency::NotBounded,
            ScanConsistency::RequestPlus => analyticsx::query_options::ScanConsistency::RequestPlus,
        }
    }
}

#[derive(Debug, Clone, TypedBuilder, Default)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct AnalyticsOptions<'a> {
    pub client_context_id: Option<&'a str>,
    pub priority: Option<bool>,
    pub read_only: Option<bool>,
    pub scan_consistency: Option<ScanConsistency>,

    pub positional_parameters: Option<&'a [&'a RawValue]>,
    pub named_parameters: Option<&'a HashMap<&'a str, &'a RawValue>>,
    pub raw: Option<&'a HashMap<&'a str, &'a RawValue>>,

    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}
