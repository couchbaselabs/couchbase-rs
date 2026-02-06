use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::durability_level::DurabilityLevel;
use crate::error;
use crate::tracing::{
    SPAN_ATTRIB_CLUSTER_NAME_KEY, SPAN_ATTRIB_CLUSTER_UUID_KEY, SPAN_ATTRIB_DB_DURABILITY,
    SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OPERATION_KEY, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
    SPAN_NAME_REQUEST_ENCODING,
};
use couchbase_core::clusterlabels::ClusterLabels;
use couchbase_core::tracingcomponent::SERVICE_VALUE_KV;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tracing::{trace_span, Level, Span};

macro_rules! record_operation_metric_event {
    (
        $duration:expr,
        $operation:expr,
        $cluster_name:expr,
        $cluster_uuid:expr,
        $error:expr
        $(, $($field:ident).+ = $value:expr )*
    ) => {{
        let cluster_name = $cluster_name;
        let cluster_uuid = $cluster_uuid;
        let error = $error;
        if let Some(error) = error {
            match (cluster_name, cluster_uuid) {
                (Some(name), Some(uuid)) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    couchbase.cluster.uuid = uuid,
                    error.type = error,
                ),
                (Some(name), None) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    error.type = error,
                ),
                (None, Some(uuid)) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.uuid = uuid,
                    error.type = error,
                ),
                (None, None) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    error.type = error,
                ),
            }
        } else {
            match (cluster_name, cluster_uuid) {
                (Some(name), Some(uuid)) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                    couchbase.cluster.uuid = uuid,
                ),
                (Some(name), None) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.name = name,
                ),
                (None, Some(uuid)) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                    couchbase.cluster.uuid = uuid,
                ),
                (None, None) => tracing::event!(
                    target: "couchbase.metrics",
                    Level::TRACE,
                    histogram.db.client.operation.duration = $duration,
                    db.operation.name = $operation,
                    $( $($field).+ = $value, )*
                ),
            }
        }
    }};
}

#[derive(Clone)]
pub(crate) struct TracingClient {
    backend: TracingClientBackend,
}

impl TracingClient {
    pub fn new(backend: TracingClientBackend) -> Self {
        Self { backend }
    }

    pub async fn create_request_encoding_span(&self) -> Span {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.create_request_encoding_span().await
            }
            TracingClientBackend::Couchbase2TracingClientBackend(_) => unimplemented!(),
        }
    }

    pub async fn record_kv_fields(&self, durability: &Option<DurabilityLevel>) {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.record_kv_fields(durability).await
            }
            TracingClientBackend::Couchbase2TracingClientBackend(_) => unimplemented!(),
        }
    }

    pub async fn record_generic_fields(&self) {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.record_generic_fields().await
            }
            TracingClientBackend::Couchbase2TracingClientBackend(_) => unimplemented!(),
        }
    }

    pub async fn execute_metered_operation<Fut, T>(
        &self,
        operation_name: &'static str,
        service: Option<&'static str>,
        keyspace: &Keyspace,
        fut: Fut,
    ) -> crate::error::Result<T>
    where
        Fut: Future<Output = crate::error::Result<T>>,
    {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client
                    .execute_metered_operation(operation_name, service, keyspace, fut)
                    .await
            }
            TracingClientBackend::Couchbase2TracingClientBackend(_) => unimplemented!(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum TracingClientBackend {
    CouchbaseTracingClientBackend(CouchbaseTracingClient),
    Couchbase2TracingClientBackend(Couchbase2TracingClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseTracingClient {
    agent_provider: CouchbaseAgentProvider,
}

impl CouchbaseTracingClient {
    pub fn new(agent_provider: CouchbaseAgentProvider) -> Self {
        Self { agent_provider }
    }

    async fn get_cluster_labels(&self) -> error::Result<Option<ClusterLabels>> {
        let agent = self.agent_provider.get_agent().await;

        Ok(CouchbaseAgentProvider::upgrade_agent(agent)?.cluster_labels())
    }

    async fn create_request_encoding_span(&self) -> Span {
        let span = trace_span!(
            SPAN_NAME_REQUEST_ENCODING,
            otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
            db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
            couchbase.cluster.uuid = tracing::field::Empty,
            couchbase.cluster.name = tracing::field::Empty,
        );

        self.record_cluster_labels(&span).await;
        span
    }

    async fn record_kv_fields(&self, durability: &Option<DurabilityLevel>) {
        let span = Span::current();
        self.record_cluster_labels(&span).await;
        self.record_durability(&span, durability);
    }

    async fn record_generic_fields(&self) {
        let span = Span::current();
        self.record_cluster_labels(&span).await;
    }

    async fn record_cluster_labels(&self, span: &Span) {
        let cluster_labels = self.get_cluster_labels().await.unwrap_or_default();

        if let Some(cluster_labels) = cluster_labels {
            if let Some(cluster_uuid) = cluster_labels.cluster_uuid {
                span.record(SPAN_ATTRIB_CLUSTER_UUID_KEY, cluster_uuid.as_str());
            }
            if let Some(cluster_name) = cluster_labels.cluster_name {
                span.record(SPAN_ATTRIB_CLUSTER_NAME_KEY, cluster_name.as_str());
            }
        }
    }

    fn record_durability(&self, span: &Span, durability_level: &Option<DurabilityLevel>) {
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

        if let Some(durability) = durability {
            span.record(SPAN_ATTRIB_DB_DURABILITY, durability);
        }
    }

    async fn execute_metered_operation<Fut, T>(
        &self,
        operation_name: &'static str,
        service: Option<&'static str>,
        keyspace: &Keyspace,
        fut: Fut,
    ) -> crate::error::Result<T>
    where
        Fut: Future<Output = crate::error::Result<T>>,
    {
        let start = std::time::Instant::now();
        let result = fut.await;
        self.record_metrics(
            operation_name,
            service,
            keyspace,
            start,
            result.as_ref().err(),
        )
        .await;
        result
    }

    async fn record_metrics(
        &self,
        operation_name: &str,
        service: Option<&str>,
        keyspace: &Keyspace,
        start: Instant,
        error: Option<&error::Error>,
    ) {
        let duration = start.elapsed().as_secs_f64();
        let cluster_labels = self.get_cluster_labels().await.unwrap_or_default();

        let cluster_name = cluster_labels
            .as_ref()
            .and_then(|labels| labels.cluster_name.as_deref());
        let cluster_uuid = cluster_labels
            .as_ref()
            .and_then(|labels| labels.cluster_uuid.as_deref());
        let error_name = error.map(|err| err.kind().metrics_name());

        match keyspace {
            Keyspace::Cluster => {
                if let Some(service) = service {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        couchbase.service = service
                    );
                } else {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name
                    );
                }
            }
            Keyspace::Bucket { bucket } => {
                if let Some(service) = service {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        couchbase.service = service,
                        db.namespace = bucket
                    );
                } else {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        db.namespace = bucket
                    );
                }
            }
            Keyspace::Scope { bucket, scope } => {
                if let Some(service) = service {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        couchbase.service = service,
                        db.namespace = bucket,
                        couchbase.scope.name = scope
                    );
                } else {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        db.namespace = bucket,
                        couchbase.scope.name = scope
                    );
                }
            }
            Keyspace::Collection {
                bucket,
                scope,
                collection,
            } => {
                if let Some(service) = service {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        couchbase.service = service,
                        db.namespace = bucket,
                        couchbase.scope.name = scope,
                        couchbase.collection.name = collection
                    );
                } else {
                    record_operation_metric_event!(
                        duration,
                        operation_name,
                        cluster_name,
                        cluster_uuid,
                        error_name,
                        db.namespace = bucket,
                        couchbase.scope.name = scope,
                        couchbase.collection.name = collection
                    );
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2TracingClient {}

impl Couchbase2TracingClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn get_cluster_labels(&self) -> Option<ClusterLabels> {
        unimplemented!()
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
