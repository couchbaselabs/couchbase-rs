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

use crate::collection::Collection;
use crate::options::kv_options::*;
use crate::results::kv_results::*;
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use crate::tracing::{
    SERVICE_VALUE_KV, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
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
        self.insert_raw_internal(id, value, flags, options).await
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
        self.replace_raw_internal(id, value, flags, options).await
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
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "upsert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn upsert_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "upsert",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    let encoding_span = self.tracing_client.create_request_encoding_span().await;
                    let (value, flags) =
                        encoding_span.in_scope(|| transcoding::json::encode(value))?;
                    drop(encoding_span);

                    self.core_kv_client
                        .upsert(id.as_ref(), &value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "upsert",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "upsert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn upsert_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "upsert",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    self.core_kv_client
                        .upsert(id.as_ref(), value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "insert",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "insert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn insert_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "insert",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    let encoding_span = self.tracing_client.create_request_encoding_span().await;
                    let (value, flags) =
                        encoding_span.in_scope(|| transcoding::json::encode(value))?;
                    drop(encoding_span);

                    self.core_kv_client
                        .insert(id.as_ref(), &value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "insert",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "insert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn insert_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "insert",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    self.core_kv_client
                        .insert(id.as_ref(), value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "replace",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "replace",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn replace_internal<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "replace",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    let encoding_span = self.tracing_client.create_request_encoding_span().await;
                    let (value, flags) =
                        encoding_span.in_scope(|| transcoding::json::encode(value))?;
                    drop(encoding_span);

                    self.core_kv_client
                        .replace(id.as_ref(), &value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "replace",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "replace",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn replace_raw_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "replace",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client
                        .record_kv_fields(&options.durability_level)
                        .await;

                    self.core_kv_client
                        .replace(id.as_ref(), value, flags, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "get",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.tracing_client
            .execute_metered_operation("get", Some(SERVICE_VALUE_KV), &self.keyspace, async move {
                let options = options.into().unwrap_or_default();
                self.tracing_client.record_generic_fields().await;

                self.core_kv_client.get(id.as_ref(), options).await
            })
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "exists",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "exists",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn exists_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        self.tracing_client
            .execute_metered_operation(
                "exists",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client.exists(id.as_ref(), options).await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get_and_touch",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "get_and_touch",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_and_touch_internal(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.tracing_client
            .execute_metered_operation(
                "get_and_touch",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client
                        .get_and_touch(id.as_ref(), expiry, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "get_and_lock",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "get_and_lock",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_and_lock_internal(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        self.tracing_client
            .execute_metered_operation(
                "get_and_lock",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client
                        .get_and_lock(id.as_ref(), lock_time, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "lookup_in",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "lookup_in",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn lookup_in_internal(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        self.tracing_client
            .execute_metered_operation(
                "lookup_in",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client
                        .lookup_in(id.as_ref(), specs, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "unlock",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "unlock",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn unlock_internal(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        self.tracing_client
            .execute_metered_operation(
                "unlock",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client.unlock(id.as_ref(), cas, options).await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "touch",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "touch",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn touch_internal(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        self.tracing_client
            .execute_metered_operation(
                "touch",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client
                        .touch(id.as_ref(), expiry, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "mutate_in",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "mutate_in",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn mutate_in_internal(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        self.tracing_client
            .execute_metered_operation(
                "mutate_in",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client
                        .mutate_in(id.as_ref(), specs, options)
                        .await
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "remove",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "remove",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.client.bucket_name(),
        couchbase.scope.name = self.client.scope_name(),
        couchbase.collection.name = self.client.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn remove_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.tracing_client
            .execute_metered_operation(
                "remove",
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                async move {
                    let options = options.into().unwrap_or_default();
                    self.tracing_client.record_generic_fields().await;

                    self.core_kv_client.remove(id.as_ref(), options).await
                },
            )
            .await
    }
}
