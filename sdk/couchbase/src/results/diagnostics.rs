/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::diagnostics::ConnectionState;
use crate::error::Error;
use crate::service_type::ServiceType;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

/// The state of a single endpoint after a ping operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PingState {
    /// The ping succeeded.
    Ok,
    /// The ping timed out.
    Timeout,
    /// The ping failed with an error.
    Error,
}

/// Detailed ping result for a single endpoint.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EndpointPingReport {
    /// The remote address of the endpoint.
    pub remote: String,
    /// The error, if the ping failed.
    pub error: Option<Error>,
    /// The round-trip latency of the ping.
    pub latency: Duration,
    /// The endpoint's connection ID.
    pub id: Option<String>,
    /// The bucket namespace, if applicable.
    pub namespace: Option<String>,
    /// Whether the ping succeeded, timed out, or errored.
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

/// The result of a [`Cluster::ping`](crate::cluster::Cluster::ping) or
/// [`Bucket::ping`](crate::bucket::Bucket::ping) operation, containing
/// latency data for each pinged endpoint grouped by service type.
///
/// Can be serialized to JSON via `Display` or `Serialize`.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
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

/// Diagnostic information about a single endpoint connection.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EndpointDiagnostics {
    /// The service type of this endpoint.
    pub service_type: ServiceType,
    /// The connection ID.
    pub id: String,
    /// The local address of the connection.
    pub local_address: Option<String>,
    /// The remote address of the endpoint.
    pub remote_address: String,
    /// Microseconds since the last activity on this connection.
    pub last_activity: Option<i64>,
    /// The current connection state.
    pub state: ConnectionState,
}

impl Serialize for EndpointDiagnostics {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("EndpointDiagnostics", 7)?;
        state.serialize_field("service_type", &self.service_type)?;
        state.serialize_field("id", &self.id)?;
        if let Some(addr) = &self.local_address {
            state.serialize_field("local_address", addr)?;
        }
        state.serialize_field("remote_address", &self.remote_address)?;
        if let Some(la) = self.last_activity {
            state.serialize_field("last_activity", &la)?;
        }
        state.serialize_field("state", &self.state)?;
        state.end()
    }
}

/// The result of a [`Cluster::diagnostics`](crate::cluster::Cluster::diagnostics) call,
/// containing the current state of all SDK connections grouped by service type.
///
/// Can be serialized to JSON via `Display` or `Serialize`.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct DiagnosticsResult {
    pub version: u32,
    pub config_rev: i64,
    pub id: String,
    pub sdk: String,
    pub services: HashMap<ServiceType, Vec<EndpointDiagnostics>>,
}

impl Display for DiagnosticsResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        serde_json::to_string(self)
            .map_err(|_| std::fmt::Error)?
            .fmt(f)
    }
}

impl From<couchbase_core::results::diagnostics::DiagnosticsResult> for DiagnosticsResult {
    fn from(value: couchbase_core::results::diagnostics::DiagnosticsResult) -> Self {
        let mut services = HashMap::new();
        for (service_type, endpoints) in value.services {
            let service_type = ServiceType::from(&service_type);
            let diagnostics: Vec<EndpointDiagnostics> = endpoints
                .into_iter()
                .map(|endpoint| EndpointDiagnostics {
                    service_type: service_type.clone(),
                    id: endpoint.id,
                    local_address: endpoint.local_address,
                    remote_address: endpoint.remote_address,
                    last_activity: endpoint.last_activity,
                    state: endpoint.state.into(),
                })
                .collect();
            services.insert(service_type, diagnostics);
        }

        DiagnosticsResult {
            version: value.version,
            config_rev: value.config_rev,
            id: value.id,
            sdk: value.sdk,
            services,
        }
    }
}

impl From<couchbase_core::results::pingreport::PingReport> for PingReport {
    fn from(value: couchbase_core::results::pingreport::PingReport) -> Self {
        let mut services = HashMap::new();
        for (service_type, endpoints) in value.services {
            let service_type = ServiceType::from(&service_type);
            let diagnostics: Vec<EndpointPingReport> = endpoints
                .into_iter()
                .map(|endpoint| EndpointPingReport {
                    remote: endpoint.remote,
                    error: endpoint.error.map(Error::from),
                    latency: endpoint.latency,
                    id: endpoint.id,
                    namespace: endpoint.namespace,
                    state: match endpoint.state {
                        couchbase_core::results::pingreport::PingState::Ok => PingState::Ok,
                        couchbase_core::results::pingreport::PingState::Timeout => {
                            PingState::Timeout
                        }
                        couchbase_core::results::pingreport::PingState::Error => PingState::Error,
                    },
                })
                .collect();
            services.insert(service_type, diagnostics);
        }

        PingReport {
            version: value.version,
            id: value.id,
            sdk: value.sdk,
            config_rev: value.config_rev,
            services,
        }
    }
}
