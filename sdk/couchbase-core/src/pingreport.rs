use crate::error::Error;
use crate::service_type::ServiceType;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
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

impl Serialize for EndpointPingReport {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EndpointPingReport", 5)?;
        state.serialize_field("remote", &self.remote)?;
        if let Some(err) = self.error.as_ref() {
            state.serialize_field("error", err.to_string().as_str())?;
        }
        state.serialize_field("latency_us", &self.latency.as_micros())?;
        if let Some(id) = &self.id {
            state.serialize_field("id", id)?;
        }
        if let Some(ns) = &self.namespace {
            state.serialize_field("namespace", ns)?;
        }
        state.serialize_field("state", &self.state)?;
        state.end()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PingReport {
    pub version: u16,
    pub id: String,
    pub sdk: String,
    pub config_rev: i64,
    pub services: HashMap<ServiceType, Vec<EndpointPingReport>>,
}

impl Display for PingReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        serde_json::to_string(self)
            .map_err(|_| std::fmt::Error)?
            .fmt(f)
    }
}
