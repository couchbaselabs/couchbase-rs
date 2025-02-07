use crate::collection::BinaryCollection;
use crate::options::kv_binary_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::MutationResult;
use tracing::instrument;
use tracing::Level;

impl BinaryCollection {
    pub async fn append(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.append_internal(id, value, options).await
    }

    pub async fn prepend(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.prepend_internal(id, value, options).await
    }

    pub async fn increment(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        self.increment_internal(id, options).await
    }

    pub async fn decrement(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        self.decrement_internal(id, options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "append",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "append",
        db.name = self.core_kv_client.bucket_name(),
        db.couchbase.collection = self.core_kv_client.collection_name(),
        db.couchbase.scope = self.core_kv_client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn append_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .append(id.as_ref(), value, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "prepend",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "prepend",
        db.name = self.core_kv_client.bucket_name(),
        db.couchbase.collection = self.core_kv_client.collection_name(),
        db.couchbase.scope = self.core_kv_client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn prepend_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .prepend(id.as_ref(), value, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "increment",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "increment",
        db.name = self.core_kv_client.bucket_name(),
        db.couchbase.collection = self.core_kv_client.collection_name(),
        db.couchbase.scope = self.core_kv_client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn increment_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client.increment(id.as_ref(), options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "decrement",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "decrement",
        db.name = self.core_kv_client.bucket_name(),
        db.couchbase.collection = self.core_kv_client.collection_name(),
        db.couchbase.scope = self.core_kv_client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn decrement_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client.decrement(id.as_ref(), options).await
    }
}
