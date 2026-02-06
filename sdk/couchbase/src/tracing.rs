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

use crate::durability_level::DurabilityLevel;
use couchbase_core::clusterlabels::ClusterLabels;
pub(crate) use couchbase_core::tracingcomponent::{
    METER_ATTRIB_BUCKET_NAME_KEY, METER_ATTRIB_CLUSTER_NAME_KEY, METER_ATTRIB_CLUSTER_UUID_KEY,
    METER_ATTRIB_COLLECTION_NAME_KEY, METER_ATTRIB_ERROR_KEY, METER_ATTRIB_OPERATION_KEY,
    METER_ATTRIB_SCOPE_NAME_KEY, METER_ATTRIB_SERVICE_KEY, METER_NAME_CB_OPERATION_DURATION,
    SERVICE_VALUE_ANALYTICS, SERVICE_VALUE_EVENTING, SERVICE_VALUE_KV, SERVICE_VALUE_MANAGEMENT,
    SERVICE_VALUE_QUERY, SERVICE_VALUE_SEARCH, SPAN_ATTRIB_CLUSTER_NAME_KEY,
    SPAN_ATTRIB_CLUSTER_UUID_KEY, SPAN_ATTRIB_DB_COLLECTION_NAME_KEY, SPAN_ATTRIB_DB_DURABILITY,
    SPAN_ATTRIB_DB_NAME_KEY, SPAN_ATTRIB_DB_SCOPE_NAME_KEY, SPAN_ATTRIB_DB_SYSTEM_KEY,
    SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_LOCAL_ID_KEY, SPAN_ATTRIB_NET_PEER_ADDRESS_KEY,
    SPAN_ATTRIB_NET_PEER_PORT_KEY, SPAN_ATTRIB_NET_TRANSPORT_KEY, SPAN_ATTRIB_NET_TRANSPORT_VALUE,
    SPAN_ATTRIB_NUM_RETRIES, SPAN_ATTRIB_OPERATION_ID_KEY, SPAN_ATTRIB_OPERATION_KEY,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE, SPAN_ATTRIB_OTEL_KIND_KEY, SPAN_ATTRIB_RETRIES,
    SPAN_ATTRIB_SERVER_DURATION_KEY, SPAN_ATTRIB_SERVICE_KEY, SPAN_NAME_DISPATCH_TO_SERVER,
    SPAN_NAME_REQUEST_ENCODING,
};

macro_rules! create_span {
    ($name:literal) => {
        SpanBuilder::new(
            $name,
            tracing::trace_span!(
                $name,
                otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
                db.operation.name = $name,
                db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
                couchbase.retries = 0,
                couchbase.cluster.name = tracing::field::Empty,
                couchbase.cluster.uuid = tracing::field::Empty,
                couchbase.service = tracing::field::Empty,
                db.namespace = tracing::field::Empty,
                couchbase.scope.name = tracing::field::Empty,
                couchbase.collection.name = tracing::field::Empty,
                couchbase.durability = tracing::field::Empty,
            ),
        )
    };
}

pub struct SpanBuilder {
    name: &'static str,
    span: tracing::Span,
}

impl SpanBuilder {
    pub fn new(name: &'static str, span: tracing::Span) -> Self {
        Self { span, name }
    }

    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    pub fn with_cluster_labels(self, cluster_labels: &Option<ClusterLabels>) -> Self {
        if let Some(labels) = cluster_labels {
            if let Some(uuid) = &labels.cluster_uuid {
                self.span
                    .record(SPAN_ATTRIB_CLUSTER_UUID_KEY, uuid.as_str());
            }
            if let Some(name) = &labels.cluster_name {
                self.span
                    .record(SPAN_ATTRIB_CLUSTER_NAME_KEY, name.as_str());
            }
        }
        self
    }

    pub fn with_durability(self, durability_level: &Option<DurabilityLevel>) -> Self {
        let durability = match durability_level {
            Some(level) if *level == DurabilityLevel::MAJORITY => Some("majority"),
            Some(level) if *level == DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE => {
                Some("majority_and_persist_active")
            }
            Some(level) if *level == DurabilityLevel::PERSIST_TO_MAJORITY => {
                Some("persist_majority")
            }
            _ => None,
        };

        if let Some(d) = durability {
            self.span.record(SPAN_ATTRIB_DB_DURABILITY, d);
        }
        self
    }

    pub fn with_keyspace(self, keyspace: &Keyspace) -> Self {
        match keyspace {
            Keyspace::Cluster => {}
            Keyspace::Bucket { bucket } => {
                self.span.record("db.namespace", bucket);
            }
            Keyspace::Scope { bucket, scope } => {
                self.span.record("db.namespace", bucket);
                self.span.record("couchbase.scope.name", scope);
            }
            Keyspace::Collection {
                bucket,
                scope,
                collection,
            } => {
                self.span.record("db.namespace", bucket);
                self.span.record("couchbase.scope.name", scope);
                self.span.record("couchbase.collection.name", collection);
            }
        }
        self
    }

    pub fn with_service(self, service: Option<&'static str>) -> Self {
        if let Some(service) = service {
            self.span.record("couchbase.service", service);
        }
        self
    }

    pub fn with_statement(self, statement: &str) -> Self {
        self.span.record("db.query.text", statement);
        self
    }

    pub fn build(self) -> tracing::Span {
        self.span
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Keyspace {
    Cluster,
    Bucket {
        bucket: String,
    },
    Scope {
        bucket: String,
        scope: String,
    },
    Collection {
        bucket: String,
        scope: String,
        collection: String,
    },
}
