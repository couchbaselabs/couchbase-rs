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

use crate::clusterlabels::ClusterLabels;
use crate::util::get_host_port_from_uri;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{instrument, span, trace_span, Instrument, Level, Span};
use url::Url;

pub const SPAN_NAME_DISPATCH_TO_SERVER: &str = "dispatch_to_server";
pub const SPAN_NAME_REQUEST_ENCODING: &str = "request_encoding";

pub const SPAN_ATTRIB_DB_SYSTEM_KEY: &str = "db.system.name";
pub const SPAN_ATTRIB_DB_SYSTEM_VALUE: &str = "couchbase";
pub const SPAN_ATTRIB_OPERATION_ID_KEY: &str = "couchbase.operation_id";
pub const SPAN_ATTRIB_OPERATION_KEY: &str = "db.operation.name";
pub const SPAN_ATTRIB_RETRIES: &str = "couchbase.retries";
pub const SPAN_ATTRIB_LOCAL_ID_KEY: &str = "couchbase.local_id";
pub const SPAN_ATTRIB_NET_TRANSPORT_KEY: &str = "network.transport";
pub const SPAN_ATTRIB_NET_TRANSPORT_VALUE: &str = "tcp";
pub const SPAN_ATTRIB_NET_REMOTE_ADDRESS_KEY: &str = "server.address";
pub const SPAN_ATTRIB_NET_REMOTE_PORT_KEY: &str = "server.port";
pub const SPAN_ATTRIB_NET_PEER_ADDRESS_KEY: &str = "network.peer.address";
pub const SPAN_ATTRIB_NET_PEER_PORT_KEY: &str = "network.peer.port";
pub const SPAN_ATTRIB_SERVER_DURATION_KEY: &str = "couchbase.server_duration";
pub const SPAN_ATTRIB_SERVICE_KEY: &str = "couchbase.service";
pub const SPAN_ATTRIB_DB_NAME_KEY: &str = "db.namespace";
pub const SPAN_ATTRIB_DB_COLLECTION_NAME_KEY: &str = "couchbase.collection.name";
pub const SPAN_ATTRIB_DB_SCOPE_NAME_KEY: &str = "couchbase.scope.name";
pub const SPAN_ATTRIB_DB_DURABILITY: &str = "couchbase.durability";
pub const SPAN_ATTRIB_NUM_RETRIES: &str = "couchbase.retries";
pub const SPAN_ATTRIB_CLUSTER_UUID_KEY: &str = "couchbase.cluster.uuid";
pub const SPAN_ATTRIB_CLUSTER_NAME_KEY: &str = "couchbase.cluster.name";

pub const METER_NAME_CB_OPERATION_DURATION: &str = "db.client.operation.duration";
pub const METER_ATTRIB_SERVICE_KEY: &str = "couchbase.service";
pub const METER_ATTRIB_OPERATION_KEY: &str = "db.operation.name";
pub const METER_ATTRIB_BUCKET_NAME_KEY: &str = "db.namespace";
pub const METER_ATTRIB_SCOPE_NAME_KEY: &str = "couchbase.scope.name";
pub const METER_ATTRIB_COLLECTION_NAME_KEY: &str = "couchbase.collection.name";
pub const METER_ATTRIB_ERROR_KEY: &str = "error.type";
pub const METER_ATTRIB_CLUSTER_UUID_KEY: &str = "couchbase.cluster.uuid";
pub const METER_ATTRIB_CLUSTER_NAME_KEY: &str = "couchbase.cluster.name";

pub const SERVICE_VALUE_KV: &str = "kv";
pub const SERVICE_VALUE_QUERY: &str = "query";
pub const SERVICE_VALUE_ANALYTICS: &str = "analytics";
pub const SERVICE_VALUE_SEARCH: &str = "search";
pub const SERVICE_VALUE_MANAGEMENT: &str = "management";
pub const SERVICE_VALUE_EVENTING: &str = "eventing";

pub const SPAN_ATTRIB_OTEL_KIND_KEY: &str = "otel.kind";
pub const SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE: &str = "client";
#[derive(Debug, Default)]
pub(crate) struct TracingComponent {
    config: RwLock<TracingComponentConfig>,
}

impl TracingComponent {
    pub(crate) fn new(config: TracingComponentConfig) -> Self {
        Self {
            config: RwLock::new(config),
        }
    }

    pub(crate) fn reconfigure(&self, state: TracingComponentConfig) {
        let mut state_guard = self.config.write().unwrap();
        *state_guard = state;
    }

    pub(crate) fn get_cluster_labels(&self) -> Option<ClusterLabels> {
        let config = self.config.read().unwrap();

        config.cluster_labels.clone()
    }

    pub(crate) fn create_dispatch_span(&self, fields: &BeginDispatchFields) -> Span {
        let span = trace_span!(
            SPAN_NAME_DISPATCH_TO_SERVER,
            otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
            db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
            network.transport = SPAN_ATTRIB_NET_TRANSPORT_VALUE,
            couchbase.cluster.uuid = tracing::field::Empty,
            couchbase.cluster.name = tracing::field::Empty,
            couchbase.server_duration = tracing::field::Empty,
            couchbase.local_id = fields.client_id,
            network.peer.address = fields.peer_addr.0,
            network.peer.port = fields.peer_addr.1,
            couchbase.operation_id = tracing::field::Empty,
        );

        self.record_cluster_labels(&span);
        span
    }

    pub(crate) fn record_cluster_labels(&self, span: &Span) {
        let cluster_labels = self.get_cluster_labels();

        if let Some(cluster_labels) = cluster_labels {
            if let Some(cluster_uuid) = cluster_labels.cluster_uuid {
                span.record(SPAN_ATTRIB_CLUSTER_UUID_KEY, cluster_uuid.as_str());
            }
            if let Some(cluster_name) = cluster_labels.cluster_name {
                span.record(SPAN_ATTRIB_CLUSTER_NAME_KEY, cluster_name.as_str());
            }
        }
    }

    pub(crate) async fn orchestrate_dispatch_span<Fut, T, F>(
        &self,
        begin_fields: BeginDispatchFields,
        operation: Fut,
        end_fields_provider: F,
    ) -> T
    where
        Fut: Future<Output = T> + Send,
        F: FnOnce(&T) -> EndDispatchFields + Send,
    {
        let span = self.create_dispatch_span(&begin_fields);
        let result = operation.instrument(span.clone()).await;
        let end_fields = end_fields_provider(&result);
        end_dispatch_span(span, end_fields);
        result
    }
}

#[derive(Debug, Clone, Default)]
pub struct TracingComponentConfig {
    pub cluster_labels: Option<ClusterLabels>,
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
        span.record(SPAN_ATTRIB_SERVER_DURATION_KEY, server_duration.as_micros());
    }

    if let Some(operation_id) = fields.operation_id {
        match operation_id {
            OperationId::String(s) => span.record(SPAN_ATTRIB_OPERATION_ID_KEY, s),
            OperationId::Number(n) => span.record(SPAN_ATTRIB_OPERATION_ID_KEY, n),
        };
    }

    drop(span);
}

#[derive(Debug)]
pub(crate) struct BeginDispatchFields {
    pub peer_addr: (String, String),
    pub client_id: Option<String>,
}

impl BeginDispatchFields {
    pub fn from_strings(peer_addr: (String, String), client_id: Option<String>) -> Self {
        Self {
            peer_addr,
            client_id,
        }
    }

    pub fn from_addrs(remote_addr: SocketAddr, client_id: String) -> Self {
        Self {
            peer_addr: (remote_addr.ip().to_string(), remote_addr.port().to_string()),
            client_id: Some(client_id),
        }
    }
}
