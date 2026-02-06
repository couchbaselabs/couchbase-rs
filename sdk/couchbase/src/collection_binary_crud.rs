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
use crate::tracing::SpanBuilder;
use crate::tracing::{
    SERVICE_VALUE_KV, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};

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

    async fn append_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("append").with_durability(&options.durability_level);

        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                span,
                self.core_kv_client.append(id.as_ref(), value, options),
            )
            .await
    }

    async fn prepend_internal(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("prepend").with_durability(&options.durability_level);

        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                span,
                self.core_kv_client.prepend(id.as_ref(), value, options),
            )
            .await
    }

    async fn increment_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("increment").with_durability(&options.durability_level);

        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                span,
                self.core_kv_client.increment(id.as_ref(), options),
            )
            .await
    }

    async fn decrement_internal(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("decrement").with_durability(&options.durability_level);

        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_KV),
                &self.keyspace,
                span,
                self.core_kv_client.decrement(id.as_ref(), options),
            )
            .await
    }
}
