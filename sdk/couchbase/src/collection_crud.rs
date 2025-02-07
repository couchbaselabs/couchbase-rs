use crate::collection::Collection;
use crate::options::kv_options::*;
use crate::results::kv_results::*;
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use crate::transcoding;
use serde::Serialize;
use std::time::Duration;
use tracing::{instrument, Level};

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.upsert_internal(id, value, options).await
    }

    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.upsert_raw_internal(id, value, flags, options).await
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.insert_internal(id, value, options).await
    }

    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.insert_raw_internal(id.as_ref(), value, flags, options)
            .await
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.replace_internal(id, value, options).await
    }

    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.replace_raw_internal(id.as_ref(), value, flags, options)
            .await
    }

    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.remove_internal(id, options).await
    }

    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.get_internal(id, options).await
    }

    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        self.exists_internal(id, options).await
    }

    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.get_and_touch_internal(id, expiry, options).await
    }

    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.get_and_lock_internal(id, lock_time, options).await
    }

    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        self.unlock_internal(id, cas, options).await
    }

    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        self.touch_internal(id, expiry, options).await
    }

    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        self.lookup_in_internal(id, specs, options).await
    }

    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        self.mutate_in_internal(id, specs, options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "upsert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "upsert",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn upsert_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        let encoding_span = self.tracing_client.create_request_encoding_span().await;
        let (value, flags) = encoding_span.in_scope(|| transcoding::json::encode(value))?;
        drop(encoding_span);

        self.core_kv_client
            .upsert(id.as_ref(), &value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "upsert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "upsert",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn upsert_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .upsert(id.as_ref(), value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "insert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "insert",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn insert_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        let encoding_span = self.tracing_client.create_request_encoding_span().await;
        let (value, flags) = encoding_span.in_scope(|| transcoding::json::encode(value))?;
        drop(encoding_span);

        self.core_kv_client
            .insert(id.as_ref(), &value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "insert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "insert",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn insert_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .insert(id.as_ref(), value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "replace",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "replace",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn replace_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        let encoding_span = self.tracing_client.create_request_encoding_span().await;
        let (value, flags) = encoding_span.in_scope(|| transcoding::json::encode(value))?;
        drop(encoding_span);

        self.core_kv_client
            .replace(id.as_ref(), &value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "replace",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "replace",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn replace_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .replace(id.as_ref(), value, flags, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "remove",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "remove",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn remove_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client.remove(id.as_ref(), options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "get",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn get_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client.get(id.as_ref(), options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "exists",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "exists",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn exists_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client.exists(id.as_ref(), options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get_and_touch",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "get_and_touch",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name, // TODO fetch from tracing wrapper
        db.couchbase.cluster_uuid, // TODO fetch from tracing wrapper
        ))]
    pub async fn get_and_touch_internal(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client
            .get_and_touch(id.as_ref(), expiry, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get_and_touch",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "get_and_touch",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn get_and_lock_internal(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client
            .get_and_lock(id.as_ref(), lock_time, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "unlock",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "unlock",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn unlock_internal(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client.unlock(id.as_ref(), cas, options).await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "touch",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "touch",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn touch_internal(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client
            .touch(id.as_ref(), expiry, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "lookup_in",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "lookup_in",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn lookup_in_internal(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client.record_generic_fields().await;

        self.core_kv_client
            .lookup_in(id.as_ref(), specs, options)
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "mutate_in",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "mutate_in",
        db.name = self.client.bucket_name(),
        db.couchbase.collection = self.client.name(),
        db.couchbase.scope = self.client.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.durability,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid
        ))]
    pub async fn mutate_in_internal(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        let options = options.into().unwrap_or_default();
        self.tracing_client
            .record_kv_fields(&options.durability_level)
            .await;

        self.core_kv_client
            .mutate_in(id.as_ref(), specs, options)
            .await
    }
}
