use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::durability_level::DurabilityLevel;
use crate::error;
use crate::tracing::{
    SPAN_ATTRIB_CLUSTER_NAME_KEY, SPAN_ATTRIB_CLUSTER_UUID_KEY, SPAN_ATTRIB_DB_DURABILITY,
    SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OPERATION_KEY, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
    SPAN_NAME_REQUEST_ENCODING,
};
use couchbase_core::clusterlabels::ClusterLabels;
use tracing::{trace_span, Span};

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

    pub async fn record_mgmt_fields(&self, path: &str) {
        match &self.backend {
            TracingClientBackend::CouchbaseTracingClientBackend(client) => {
                client.record_mgmt_fields(path).await
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

    async fn record_mgmt_fields(&self, path: &str) {
        let span = Span::current();
        self.record_cluster_labels(&span).await;
        span.record(SPAN_ATTRIB_OPERATION_KEY, path);
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
