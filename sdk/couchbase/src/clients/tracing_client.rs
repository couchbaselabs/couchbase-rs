use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::durability_level::DurabilityLevel;
use crate::error;
use crate::error::ErrorKind;
use crate::tracing::{
    Keyspace, SpanBuilder, SPAN_ATTRIB_CLUSTER_NAME_KEY, SPAN_ATTRIB_CLUSTER_UUID_KEY,
    SPAN_ATTRIB_DB_DURABILITY, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OPERATION_KEY,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE, SPAN_NAME_REQUEST_ENCODING,
};
use couchbase_core::clusterlabels::ClusterLabels;
use couchbase_core::tracingcomponent::{record_metrics, SERVICE_VALUE_KV};
use std::sync::Arc;
use std::time::Instant;
use tracing::{trace_span, Instrument, Level, Span};

pub(crate) struct OperationContext<'a> {
    span: Span,
    operation_name: &'static str,
    service: Option<&'static str>,
    cluster_labels: Option<ClusterLabels>,
    keyspace: Keyspace<'a>,
    start: Instant,
    completed: bool,
}

impl OperationContext<'_> {
    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn end_operation(mut self, err: Option<&crate::error::Error>) {
        self.completed = true;
        self.span.record(
            "otel.status_code",
            if err.is_some() { "error" } else { "ok" },
        );

        record_metrics(
            self.operation_name,
            self.service,
            &self.keyspace,
            &self.cluster_labels,
            self.start,
            err,
        );
    }
}

impl Drop for OperationContext<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        self.span.record("otel.status_code", "error");

        record_metrics(
            self.operation_name,
            self.service,
            &self.keyspace,
            &self.cluster_labels,
            self.start,
            Some(&error::Error::new(ErrorKind::RequestCanceled)),
        );
    }
}

#[derive(Clone)]
pub(crate) struct TracingClient {
    backend: TracingClientBackend,
}

impl TracingClient {
    pub fn new(backend: TracingClientBackend) -> Self {
        Self { backend }
    }

    pub async fn begin_operation<'a>(
        &self,
        service: Option<&'static str>,
        keyspace: Keyspace<'a>,
        span: SpanBuilder,
    ) -> OperationContext<'a> {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.begin_operation(service, keyspace, span).await
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
            target: "couchbase::tracing",
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

    async fn begin_operation<'a>(
        &self,
        service: Option<&'static str>,
        keyspace: Keyspace<'a>,
        mut span: SpanBuilder,
    ) -> OperationContext<'a> {
        let operation_name = span.name();

        // Fast-path: if tracing is not enabled, avoid fetching the cluster labels.
        let tracing_enabled = tracing::enabled!(Level::TRACE);

        let cluster_labels: Option<ClusterLabels> = if tracing_enabled {
            self.get_cluster_labels().await.unwrap_or_default()
        } else {
            None
        };

        let built_span = span
            .with_cluster_labels(&cluster_labels)
            .with_service(service)
            .with_keyspace(&keyspace)
            .build();

        OperationContext {
            span: built_span,
            operation_name,
            service,
            cluster_labels,
            keyspace,
            start: Instant::now(),
            completed: false,
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
