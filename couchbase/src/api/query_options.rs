use crate::api::collection::MutationState;
use serde_derive::Serialize;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Default, Serialize)]
pub struct QueryOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_consistency: Option<QueryScanConsistency>,
    #[serde(skip)]
    pub(crate) adhoc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(serialize_with = "crate::default_client_context_id")]
    pub(crate) client_context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_batch: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::convert_duration_for_golang")]
    pub(crate) scan_wait: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) readonly: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<QueryProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::convert_mutation_state")]
    pub(crate) consistent_with: Option<MutationState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "args")]
    pub(crate) positional_parameters: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "crate::convert_named_params")]
    pub(crate) named_parameters: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The statement is not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the query engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    pub(crate) statement: Option<String>,
}

impl QueryOptions {
    timeout!();

    pub fn scan_consistency(mut self, scan_consistency: QueryScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn adhoc(mut self, adhoc: bool) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub fn client_context_id(mut self, client_context_id: String) -> Self {
        self.client_context_id = Some(client_context_id);
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    pub fn pipeline_batch(mut self, pipeline_batch: u32) -> Self {
        self.pipeline_batch = Some(pipeline_batch);
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: u32) -> Self {
        self.pipeline_cap = Some(pipeline_cap);
        self
    }

    pub fn scan_cap(mut self, scan_cap: u32) -> Self {
        self.scan_cap = Some(scan_cap);
        self
    }

    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = Some(readonly);
        self
    }

    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn profile(mut self, profile: QueryProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn consistent_with(mut self, consistent_with: MutationState) -> Self {
        self.consistent_with = Some(consistent_with);
        self
    }

    pub fn positional_parameters<T>(mut self, positional_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let positional_parameters = match serde_json::to_value(positional_parameters) {
            Ok(Value::Array(a)) => a,
            Ok(_) => panic!("Only arrays are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.positional_parameters = Some(positional_parameters);
        self
    }

    pub fn named_parameters<T>(mut self, named_parameters: T) -> Self
    where
        T: serde::Serialize,
    {
        let named_parameters = match serde_json::to_value(named_parameters) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode positional parameters"),
        };
        self.named_parameters = Some(named_parameters);
        self
    }

    pub fn raw<T>(mut self, raw: T) -> Self
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode raw parameters"),
        };
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub enum QueryScanConsistency {
    #[serde(rename = "not_bounded")]
    NotBounded,
    #[serde(rename = "request_plus")]
    RequestPlus,
}

#[derive(Debug, Serialize, Clone, Copy)]
pub enum QueryProfile {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "phases")]
    Phases,
    #[serde(rename = "timings")]
    Timings,
}
