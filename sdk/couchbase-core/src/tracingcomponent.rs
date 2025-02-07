use crate::util::get_host_port_from_uri;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{instrument, span, trace_span, Level, Span};
use url::Url;

#[derive(Debug, Default)]
pub(crate) struct TracingComponent {
    config: Mutex<TracingComponentConfig>,
}

impl TracingComponent {
    pub(crate) fn new(config: TracingComponentConfig) -> Self {
        Self {
            config: Mutex::new(config),
        }
    }

    pub(crate) fn reconfigure(&self, state: TracingComponentConfig) {
        let mut state_guard = self.config.lock().unwrap();
        *state_guard = state;
    }

    pub(crate) fn get_cluster_labels(&self) -> Option<ClusterLabels> {
        let config = self.config.lock().unwrap();

        config.cluster_labels.clone()
    }

    pub(crate) fn create_dispatch_span(&self, fields: &BeginDispatchFields) -> Span {
        let span = trace_span!(
            "dispatch_to_server",
            otel.kind = "client",
            db.system = "couchbase",
            net.transport = "IP.TCP",
            db.couchbase.cluster_uuid = tracing::field::Empty,
            db.couchbase.cluster_name = tracing::field::Empty,
            db.couchbase.server_duration = tracing::field::Empty,
            db.couchbase.local_id = fields.client_id,
            net.host.name = tracing::field::Empty,
            net.host.port = tracing::field::Empty,
            net.peer.name = fields.peer_addr.0,
            net.peer.port = fields.peer_addr.1,
            db.couchbase.operation_id = tracing::field::Empty,
        );

        if let Some(local_addr) = &fields.local_addr {
            span.record("net.host.name", &local_addr.0);
            span.record("net.host.port", &local_addr.1);
        }

        self.record_cluster_labels(&span);
        span
    }

    pub(crate) fn record_cluster_labels(&self, span: &Span) {
        let cluster_labels = self.get_cluster_labels();

        if let Some(cluster_labels) = cluster_labels {
            if let Some(cluster_uuid) = cluster_labels.cluster_uuid {
                span.record("db.couchbase.cluster_uuid", cluster_uuid.as_str());
            }
            if let Some(cluster_name) = cluster_labels.cluster_name {
                span.record("db.couchbase.cluster_name", cluster_name.as_str());
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TracingComponentConfig {
    pub cluster_labels: Option<ClusterLabels>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterLabels {
    pub cluster_uuid: Option<String>,
    pub cluster_name: Option<String>,
}

pub enum OperationId {
    String(String),
    Number(u64),
}

impl OperationId {
    pub fn from_u32(n: u32) -> Self {
        Self::Number(n as u64)
    }

    pub fn from_string(s: String) -> Self {
        Self::String(s)
    }
}

pub struct EndDispatchFields {
    pub server_duration: Option<Duration>,
    pub operation_id: Option<OperationId>,
}

impl EndDispatchFields {
    pub fn new(server_duration: Option<Duration>, operation_id: Option<OperationId>) -> Self {
        Self {
            server_duration,
            operation_id,
        }
    }

    pub fn server_duration(mut self, server_duration: Option<Duration>) -> Self {
        self.server_duration = server_duration;
        self
    }

    pub fn operation_id(mut self, operation_id: Option<OperationId>) -> Self {
        self.operation_id = operation_id;
        self
    }
}

pub fn end_dispatch_span(span: Span, fields: EndDispatchFields) {
    if let Some(server_duration) = fields.server_duration {
        span.record("db.couchbase.server_duration", server_duration.as_micros());
    }

    if let Some(operation_id) = fields.operation_id {
        match operation_id {
            OperationId::String(s) => span.record("db.couchbase.operation_id", s),
            OperationId::Number(n) => span.record("db.couchbase.operation_id", n),
        };
    }

    drop(span);
}

#[derive(Debug)]
pub(crate) struct BeginDispatchFields {
    pub local_addr: Option<(String, String)>,
    pub peer_addr: (String, String),
    pub client_id: Option<String>,
}

impl BeginDispatchFields {
    pub fn from_strings(
        local_addr: Option<(String, String)>,
        peer_addr: (String, String),
        client_id: Option<String>,
    ) -> Self {
        Self {
            local_addr,
            peer_addr,
            client_id,
        }
    }

    pub fn from_addrs(local_addr: SocketAddr, remote_addr: SocketAddr, client_id: String) -> Self {
        Self {
            local_addr: Some((local_addr.ip().to_string(), local_addr.port().to_string())),
            peer_addr: (remote_addr.ip().to_string(), remote_addr.port().to_string()),
            client_id: Some(client_id),
        }
    }
}
