use crate::analyticsx::query_respreader::Status;
use crate::analyticsx::query_result;
use crate::helpers::durations::parse_duration_from_golang_string;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct QueryErrorResponse {
    #[serde(default)]
    pub errors: Vec<QueryError>,
}

#[derive(Debug, Deserialize)]
pub struct QueryWarning {
    pub code: Option<u32>,
    pub msg: Option<String>,
}

impl From<QueryWarning> for query_result::Warning {
    fn from(warning: QueryWarning) -> Self {
        query_result::Warning {
            code: warning.code,
            msg: warning.msg,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryError {
    pub code: Option<u32>,
    pub msg: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    pub request_id: Option<String>,
    #[serde(rename = "clientContextID")]
    pub client_context_id: Option<String>,
    pub status: Option<Status>,
    #[serde(default)]
    pub errors: Option<Vec<QueryError>>,
    #[serde(default)]
    pub warnings: Option<Vec<QueryWarning>>,
    pub metrics: Option<QueryMetrics>,
    pub signature: Option<Box<RawValue>>,
}

impl From<QueryMetaData> for query_result::MetaData {
    fn from(meta: QueryMetaData) -> Self {
        let warnings = if let Some(warnings) = meta.warnings {
            warnings
                .into_iter()
                .map(query_result::Warning::from)
                .collect()
        } else {
            vec![]
        };

        query_result::MetaData {
            request_id: meta.request_id,
            client_context_id: meta.client_context_id,
            status: meta.status,
            warnings,
            metrics: meta
                .metrics
                .map(query_result::Metrics::from)
                .unwrap_or_default(),
            signature: meta.signature,
        }
    }
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
    #[serde(rename = "errorCount")]
    pub error_count: Option<u64>,
    #[serde(rename = "warningCount")]
    pub warning_count: Option<u64>,
}

impl From<QueryMetrics> for query_result::Metrics {
    fn from(metrics: QueryMetrics) -> Self {
        let elapsed_time = if let Some(elapsed) = metrics.elapsed_time {
            parse_duration_from_golang_string(&elapsed).unwrap_or_default()
        } else {
            Duration::default()
        };

        let execution_time = if let Some(execution) = metrics.execution_time {
            parse_duration_from_golang_string(&execution).unwrap_or_default()
        } else {
            Duration::default()
        };

        query_result::Metrics {
            elapsed_time,
            execution_time,
            result_count: metrics.result_count.unwrap_or_default(),
            result_size: metrics.result_size.unwrap_or_default(),
            error_count: metrics.error_count.unwrap_or_default(),
            warning_count: metrics.warning_count.unwrap_or_default(),
        }
    }
}
