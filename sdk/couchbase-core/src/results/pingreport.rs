use crate::error::Error;
use crate::service_type::ServiceType;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PingState {
    Ok,
    Timeout,
    Error,
}

#[derive(Debug, Clone)]
pub struct EndpointPingReport {
    pub remote: String,
    pub error: Option<Error>,
    pub latency: Duration,
    pub id: Option<String>,
    pub namespace: Option<String>,
    pub state: PingState,
}

#[derive(Debug, Clone)]
pub struct PingReport {
    pub version: u16,
    pub id: String,
    pub sdk: String,
    pub config_rev: i64,
    pub services: HashMap<ServiceType, Vec<EndpointPingReport>>,
}
