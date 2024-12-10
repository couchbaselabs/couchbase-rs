use crate::analyticsx::query_respreader::Status;
use serde_json::value::RawValue;
use std::time::Duration;

#[derive(Debug)]
pub struct Warning {
    pub code: Option<u32>,
    pub msg: Option<String>,
}

#[derive(Debug)]
pub struct MetaData {
    pub request_id: Option<String>,
    pub client_context_id: Option<String>,
    pub status: Option<Status>,
    pub warnings: Vec<Warning>,
    pub metrics: Metrics,
    pub signature: Option<Box<RawValue>>,
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub elapsed_time: Duration,
    pub execution_time: Duration,
    pub result_count: u64,
    pub result_size: u64,
    pub error_count: u64,
    pub warning_count: u64,
}
