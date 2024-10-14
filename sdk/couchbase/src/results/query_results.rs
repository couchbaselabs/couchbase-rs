use crate::error;
use couchbase_core::querycomponent::QueryResultStream;
use couchbase_core::queryx;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::time::Duration;

pub struct QueryResult {
    wrapped: QueryResultStream,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct QueryWarning {
    pub code: u32,
    pub message: String,
}

impl From<queryx::query_result::Warning> for QueryWarning {
    fn from(warning: queryx::query_result::Warning) -> Self {
        Self {
            code: warning.code,
            message: warning.message,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum QueryStatus {
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

impl From<queryx::query_result::Status> for QueryStatus {
    fn from(status: queryx::query_result::Status) -> Self {
        match status {
            queryx::query_result::Status::Running => Self::Running,
            queryx::query_result::Status::Success => Self::Success,
            queryx::query_result::Status::Errors => Self::Errors,
            queryx::query_result::Status::Completed => Self::Completed,
            queryx::query_result::Status::Stopped => Self::Stopped,
            queryx::query_result::Status::Timeout => Self::Timeout,
            queryx::query_result::Status::Closed => Self::Closed,
            queryx::query_result::Status::Fatal => Self::Fatal,
            queryx::query_result::Status::Aborted => Self::Aborted,
            queryx::query_result::Status::Unknown => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct QueryMetrics {
    pub elapsed_time: Duration,
    pub execution_time: Duration,
    pub result_count: u64,
    pub result_size: u64,
    pub mutation_count: u64,
    pub sort_count: u64,
    pub error_count: u64,
    pub warning_count: u64,
}

impl From<queryx::query_result::Metrics> for QueryMetrics {
    fn from(metrics: queryx::query_result::Metrics) -> Self {
        Self {
            elapsed_time: metrics.elapsed_time,
            execution_time: metrics.execution_time,
            result_count: metrics.result_count,
            result_size: metrics.result_size,
            mutation_count: metrics.mutation_count,
            sort_count: metrics.sort_count,
            error_count: metrics.error_count,
            warning_count: metrics.warning_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryMetaData {
    pub request_id: String,
    pub client_context_id: String,
    pub status: QueryStatus,
    pub metrics: QueryMetrics,
    pub signature: Option<Value>,
    pub warnings: Vec<QueryWarning>,
    pub profile: Option<Value>,
}

impl From<queryx::query_result::MetaData> for QueryMetaData {
    fn from(meta: queryx::query_result::MetaData) -> Self {
        Self {
            request_id: meta.request_id,
            client_context_id: meta.client_context_id,
            status: meta.status.into(),
            metrics: meta.metrics.into(),
            signature: meta.signature,
            warnings: meta.warnings.into_iter().map(|w| w.into()).collect(),
            profile: meta.profile,
        }
    }
}

impl From<QueryResultStream> for QueryResult {
    fn from(wrapped: QueryResultStream) -> Self {
        Self { wrapped }
    }
}

impl Stream for QueryResult {
    type Item = error::Result<bytes::Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.wrapped.poll_next_unpin(cx).map_err(|e| e.into())
    }
}

impl QueryResult {
    pub async fn metadata(self) -> error::Result<QueryMetaData> {
        Ok(self.wrapped.metadata().await?.into())
    }
}