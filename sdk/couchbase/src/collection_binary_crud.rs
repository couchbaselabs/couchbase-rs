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

use crate::collection::BinaryCollection;
use crate::options::kv_binary_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::MutationResult;
use crate::tracing::{
    SERVICE_VALUE_KV, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use tracing::{instrument, Level};

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
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "append",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.core_kv_client.bucket_name(),
        couchbase.scope.name = self.core_kv_client.scope_name(),
        couchbase.collection.name = self.core_kv_client.collection_name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
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
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "prepend",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.core_kv_client.bucket_name(),
        couchbase.scope.name = self.core_kv_client.scope_name(),
        couchbase.collection.name = self.core_kv_client.collection_name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
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
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "increment",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.core_kv_client.bucket_name(),
        couchbase.scope.name = self.core_kv_client.scope_name(),
        couchbase.collection.name = self.core_kv_client.collection_name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
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
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "decrement",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.core_kv_client.bucket_name(),
        couchbase.scope.name = self.core_kv_client.scope_name(),
        couchbase.collection.name = self.core_kv_client.collection_name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.durability,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn decrement_internal(
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
