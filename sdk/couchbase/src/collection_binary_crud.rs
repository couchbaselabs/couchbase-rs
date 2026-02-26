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
use crate::tracing::SERVICE_VALUE_KV;
use couchbase_core::create_span;
use tracing::Instrument;

impl BinaryCollection {
    pub async fn append(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("append").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .append(id.as_ref(), value, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn prepend(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("prepend").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .prepend(id.as_ref(), value, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn increment(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("increment").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .increment(id.as_ref(), options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn decrement(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> crate::error::Result<CounterResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("decrement").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .decrement(id.as_ref(), options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
