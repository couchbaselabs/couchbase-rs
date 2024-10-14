use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Success,
    Errors,
    Completed,
    Stopped,
    Timeout,
    Closed,
    Fatal,
    Aborted,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct EarlyMetaData {
    pub prepared: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetaData {
    pub prepared: Option<String>,
    pub request_id: String,
    pub client_context_id: String,
    pub status: Status,
    pub metrics: Metrics,
    pub signature: Option<Value>,
    pub warnings: Vec<Warning>,
    pub profile: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct Warning {
    pub code: u32,
    pub message: String,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub elapsed_time: Duration,
    pub execution_time: Duration,
    pub result_count: u64,
    pub result_size: u64,
    pub mutation_count: u64,
    pub sort_count: u64,
    pub error_count: u64,
    pub warning_count: u64,
}
