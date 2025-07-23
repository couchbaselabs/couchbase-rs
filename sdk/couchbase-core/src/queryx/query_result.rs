use std::fmt::{Display, Formatter};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
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

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Running => write!(f, "running"),
            Status::Success => write!(f, "success"),
            Status::Errors => write!(f, "errors"),
            Status::Completed => write!(f, "completed"),
            Status::Stopped => write!(f, "stopped"),
            Status::Timeout => write!(f, "timeout"),
            Status::Closed => write!(f, "closed"),
            Status::Fatal => write!(f, "fatal"),
            Status::Aborted => write!(f, "aborted"),
            Status::Unknown => write!(f, "unknown"),
        }
    }
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
    pub signature: Option<Box<RawValue>>,
    pub warnings: Vec<Warning>,
    pub profile: Option<Box<RawValue>>,
}

impl Display for MetaData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prepared: {:?}, request_id: {}, client_context_id: {}, status: {}, metrics: {}, signature: {:?}, warnings: {:?}, profile: {:?}",
            self.prepared,
            self.request_id,
            self.client_context_id,
            self.status,
            self.metrics,
            self.signature,
            self.warnings,
            self.profile
        )
    }
}

#[derive(Debug, Clone)]
pub struct Warning {
    pub code: u32,
    pub message: String,
}

impl Display for Warning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "code: {}, message: {}", self.code, self.message)
    }
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

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "elapsed_time: {:?}, execution_time: {:?}, result_count: {}, result_size: {}, mutation_count: {}, sort_count: {}, error_count: {}, warning_count: {}",
            self.elapsed_time,
            self.execution_time,
            self.result_count,
            self.result_size,
            self.mutation_count,
            self.sort_count,
            self.error_count,
            self.warning_count
        )
    }
}
