use serde_derive::Serialize;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Default, Serialize)]
pub struct AnalyticsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_consistency: Option<AnalyticsScanConsistency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(serialize_with = "crate::default_client_context_id")]
    pub(crate) client_context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "args")]
    pub(crate) positional_parameters: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "crate::convert_named_params")]
    pub(crate) named_parameters: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) readonly: Option<bool>,
    #[serde(skip)]
    pub(crate) priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
    // The statement is not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the analytics engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    pub(crate) statement: Option<String>,
}

impl AnalyticsOptions {
    timeout!();

    pub fn scan_consistency(mut self, scan_consistency: AnalyticsScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn client_context_id(mut self, client_context_id: String) -> Self {
        self.client_context_id = Some(client_context_id);
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = Some(readonly);
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

    pub fn priority(mut self, priority: bool) -> Self {
        self.priority = Some(if priority { -1 } else { 0 });
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

#[derive(Debug, Serialize)]
pub enum AnalyticsScanConsistency {
    #[serde(rename = "not_bounded")]
    NotBounded,
    #[serde(rename = "request_plus")]
    RequestPlus,
}
