use crate::error;
use couchbase_core::analyticscomponent::AnalyticsResultStream;
use couchbase_core::analyticsx;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct AnalyticsResult {
    wrapped: AnalyticsResultStream,
}

impl From<AnalyticsResultStream> for AnalyticsResult {
    fn from(wrapped: AnalyticsResultStream) -> Self {
        Self { wrapped }
    }
}

#[derive(Debug)]
pub struct AnalyticsWarning<'a> {
    pub code: Option<u32>,
    pub msg: Option<&'a str>,
}

impl<'a> From<&'a analyticsx::query_result::Warning> for AnalyticsWarning<'a> {
    fn from(warning: &'a analyticsx::query_result::Warning) -> Self {
        Self {
            code: warning.code,
            msg: warning.msg.as_deref(),
        }
    }
}

#[derive(Debug)]
pub struct AnalyticsMetaData<'a> {
    pub request_id: Option<&'a str>,
    pub client_context_id: Option<&'a str>,
    pub status: Option<AnalyticsStatus>,
    pub warnings: Vec<AnalyticsWarning<'a>>,
    pub metrics: AnalyticsMetrics,
    pub signature: Option<&'a RawValue>,
}

impl<'a> From<&'a analyticsx::query_result::MetaData> for AnalyticsMetaData<'a> {
    fn from(meta: &'a analyticsx::query_result::MetaData) -> Self {
        Self {
            request_id: meta.request_id.as_deref(),
            client_context_id: meta.client_context_id.as_deref(),
            status: meta.status.as_ref().map(AnalyticsStatus::from),
            warnings: meta.warnings.iter().map(|w| w.into()).collect(),
            metrics: AnalyticsMetrics::from(&meta.metrics),
            signature: meta.signature.as_deref(),
        }
    }
}

#[derive(Debug, Default)]
pub struct AnalyticsMetrics {
    pub elapsed_time: Duration,
    pub execution_time: Duration,
    pub result_count: u64,
    pub result_size: u64,
    pub error_count: u64,
    pub warning_count: u64,
}

impl From<&analyticsx::query_result::Metrics> for AnalyticsMetrics {
    fn from(metrics: &analyticsx::query_result::Metrics) -> Self {
        Self {
            elapsed_time: metrics.elapsed_time,
            execution_time: metrics.execution_time,
            result_count: metrics.result_count,
            result_size: metrics.result_size,
            error_count: metrics.error_count,
            warning_count: metrics.warning_count,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum AnalyticsStatus {
    Running,
    Success,
    Failed,
    Timeout,
    Fatal,
    Unknown,
}

impl From<&analyticsx::query_respreader::Status> for AnalyticsStatus {
    fn from(status: &analyticsx::query_respreader::Status) -> Self {
        match status {
            analyticsx::query_respreader::Status::Running => Self::Running,
            analyticsx::query_respreader::Status::Success => Self::Success,
            analyticsx::query_respreader::Status::Failed => Self::Failed,
            analyticsx::query_respreader::Status::Timeout => Self::Timeout,
            analyticsx::query_respreader::Status::Fatal => Self::Fatal,
            analyticsx::query_respreader::Status::Unknown => Self::Unknown,
        }
    }
}

struct AnalyticsRows<'a, V: DeserializeOwned> {
    wrapped: &'a mut AnalyticsResultStream,
    phantom_data: PhantomData<&'a V>,
}

impl<'a, V: DeserializeOwned> Stream for AnalyticsRows<'a, V> {
    type Item = error::Result<V>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let row = match self.wrapped.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row))) => row,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        let row = serde_json::from_slice(&row).map_err(error::Error::from);
        Poll::Ready(Some(row))
    }
}

impl AnalyticsResult {
    pub async fn metadata(&self) -> error::Result<AnalyticsMetaData> {
        Ok(self.wrapped.metadata()?.into())
    }

    pub fn rows<'a, V: DeserializeOwned + 'a>(
        &'a mut self,
    ) -> impl Stream<Item = error::Result<V>> + 'a {
        AnalyticsRows {
            wrapped: &mut self.wrapped,
            phantom_data: PhantomData,
        }
    }
}
