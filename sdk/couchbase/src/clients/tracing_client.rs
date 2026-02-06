use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::durability_level::DurabilityLevel;
use crate::error;
use crate::tracing::{
    Keyspace, SpanBuilder, SPAN_ATTRIB_CLUSTER_NAME_KEY, SPAN_ATTRIB_CLUSTER_UUID_KEY,
    SPAN_ATTRIB_DB_DURABILITY, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OPERATION_KEY,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE, SPAN_NAME_REQUEST_ENCODING,
};
use couchbase_core::clusterlabels::ClusterLabels;
use couchbase_core::tracingcomponent::SERVICE_VALUE_KV;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tracing::{trace_span, Instrument, Level, Span};

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

    pub async fn execute_observable_operation<Fut, T>(
        &self,
        service: Option<&'static str>,
        keyspace: &Keyspace,
        span: SpanBuilder,
        fut: Fut,
    ) -> crate::error::Result<T>
    where
        Fut: Future<Output = crate::error::Result<T>>,
    {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client
                    .execute_observable_operation(service, keyspace, span, fut)
                    .await
            }
            TracingClientBackend::Couchbase2TracingClientBackend(_) => unimplemented!(),
        }
    }

    pub async fn with_request_encoding_span<T>(
        &self,
        f: impl FnOnce() -> crate::error::Result<T>,
    ) -> crate::error::Result<T> {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.with_request_encoding_span(f).await
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

    async fn with_request_encoding_span<T>(
        &self,
        f: impl FnOnce() -> crate::error::Result<T>,
    ) -> crate::error::Result<T> {
        let span = trace_span!(
            SPAN_NAME_REQUEST_ENCODING,
            otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
            db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
            couchbase.cluster.uuid = tracing::field::Empty,
            couchbase.cluster.name = tracing::field::Empty,
        );
        let cluster_labels = self.get_cluster_labels().await.unwrap_or_default();
        let span = SpanBuilder::new(SPAN_NAME_REQUEST_ENCODING, span)
            .with_cluster_labels(&cluster_labels)
            .build();

        span.in_scope(f)
    }

    async fn execute_observable_operation<Fut, T>(
        &self,
        service: Option<&'static str>,
        keyspace: &Keyspace,
        mut span: SpanBuilder,
        fut: Fut,
    ) -> crate::error::Result<T>
    where
        Fut: Future<Output = crate::error::Result<T>>,
    {
        let operation_name = span.name();
        let cluster_labels = self.get_cluster_labels().await.unwrap_or_default();
        let span = span
            .with_cluster_labels(&cluster_labels)
            .with_service(service)
            .with_keyspace(keyspace)
            .build();

        let start = Instant::now();
        let result = fut.instrument(span).await;
        Self::record_metrics(
            operation_name,
            service,
            keyspace,
            cluster_labels,
            start,
            result.as_ref().err(),
        );

        result
    }

    fn record_metrics(
        operation_name: &str,
        service: Option<&str>,
        keyspace: &Keyspace,
        cluster_labels: Option<ClusterLabels>,
        start: Instant,
        error: Option<&error::Error>,
    ) {
        let duration = start.elapsed().as_secs_f64();

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
