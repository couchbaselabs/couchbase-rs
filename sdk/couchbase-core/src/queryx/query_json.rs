use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

use crate::queryx::query_result::Status;

#[derive(Debug, Deserialize)]
pub struct QueryErrorResponse {
    #[serde(default)]
    pub errors: Vec<QueryError>,
}

#[derive(Debug, Deserialize)]
pub struct QueryEarlyMetaData {
    pub prepared: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(flatten)]
    pub early_meta_data: QueryEarlyMetaData,
    #[serde(rename = "requestID")]
    pub request_id: Option<String>,
    #[serde(rename = "clientContextID")]
    pub client_context_id: Option<String>,
    pub status: Status,
    #[serde(default)]
    pub errors: Vec<QueryError>,
    #[serde(default)]
    pub warnings: Vec<QueryWarning>,
    pub metrics: Option<QueryMetrics>,
    pub profile: Option<Value>,
    pub signature: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    pub elapsed_time: Option<String>,
    #[serde(rename = "executionTime")]
    pub execution_time: Option<String>,
    #[serde(rename = "resultCount")]
    pub result_count: Option<u64>,
    #[serde(rename = "resultSize")]
    pub result_size: Option<u64>,
    #[serde(rename = "mutationCount")]
    pub mutation_count: Option<u64>,
    #[serde(rename = "sortCount")]
    pub sort_count: Option<u64>,
    #[serde(rename = "errorCount")]
    pub error_count: Option<u64>,
    #[serde(rename = "warningCount")]
    pub warning_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct QueryWarning {
    pub code: Option<u32>,
    pub msg: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueryError {
    pub code: u32,
    pub msg: String,
    #[serde(default)]
    pub reason: std::collections::HashMap<String, Value>,
    pub retry: Option<bool>,
}
