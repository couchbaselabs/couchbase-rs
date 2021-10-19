use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::Value;
use std::time::Duration;

#[derive(Debug)]
pub struct QueryResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<QueryMetaData>>,
}

impl QueryResult {
    pub(crate) fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<QueryMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> CouchbaseResult<QueryMetaData> {
        self.meta
            .take()
            .expect("Can not consume metadata twice!")
            .await
            .map_err(|e| {
                let mut ctx = ErrorContext::default();
                ctx.insert("error", Value::String(e.to_string()));
                CouchbaseError::RequestCanceled { ctx }
            })
    }
}

#[derive(Debug, Copy, Clone, Deserialize, Eq, PartialEq)]
pub enum QueryStatus {
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "errors")]
    Errors,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "stopped")]
    Stopped,
    #[serde(rename = "timeout")]
    Timeout,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "fatal")]
    Fatal,
    #[serde(rename = "aborted")]
    Aborted,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct QueryWarning {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
    metrics: Option<QueryMetrics>,
    status: QueryStatus,
    warnings: Option<Vec<QueryWarning>>,
    signature: Option<Value>,
    profile: Option<Value>,
}

impl QueryMetaData {
    pub fn metrics(&self) -> Option<&QueryMetrics> {
        self.metrics.as_ref()
    }

    pub fn request_id(&self) -> &str {
        self.request_id.as_ref()
    }

    pub fn client_context_id(&self) -> &str {
        self.client_context_id.as_ref()
    }

    pub fn status(&self) -> QueryStatus {
        self.status
    }

    pub fn warnings(&self) -> Option<impl IntoIterator<Item = &QueryWarning>> {
        self.warnings.as_ref()
    }

    pub fn signature<T>(&self) -> Option<CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        Some(
            serde_json::from_value(self.signature.clone()?)
                .map_err(|e| CouchbaseError::decoding_failure_from_serde(e)),
        )
    }

    pub fn profile<T>(&self) -> Option<CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        Some(
            serde_json::from_value(self.signature.clone()?)
                .map_err(|e| CouchbaseError::decoding_failure_from_serde(e)),
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "sortCount", default)]
    sort_count: usize,
    #[serde(rename = "resultCount")]
    result_count: usize,
    #[serde(rename = "resultSize")]
    result_size: usize,
    #[serde(rename = "mutationCount", default)]
    mutation_count: usize,
    #[serde(rename = "errorCount", default)]
    error_count: usize,
    #[serde(rename = "warningCount", default)]
    warning_count: usize,
}

impl QueryMetrics {
    pub fn elapsed_time(&self) -> Duration {
        match parse_duration::parse(&self.elapsed_time) {
            Ok(d) => d,
            Err(_e) => Duration::from_secs(0),
        }
    }

    pub fn execution_time(&self) -> Duration {
        match parse_duration::parse(&self.execution_time) {
            Ok(d) => d,
            Err(_e) => Duration::from_secs(0),
        }
    }

    pub fn sort_count(&self) -> usize {
        self.sort_count
    }

    pub fn result_count(&self) -> usize {
        self.result_count
    }

    pub fn result_size(&self) -> usize {
        self.result_size
    }

    pub fn mutation_count(&self) -> usize {
        self.mutation_count
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }

    pub fn warning_count(&self) -> usize {
        self.warning_count
    }
}
