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
use crate::tracing::SERVICE_VALUE_KV;
use crate::transcoding;
use couchbase_core::create_span;
use serde::Serialize;
use std::time::Duration;
use tracing::Instrument;

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("upsert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .upsert(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("upsert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .upsert(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("insert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .insert(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("insert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .insert(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("replace").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .replace(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("replace").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .replace(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("remove"),
            )
            .await;
        let result = self
            .core_kv_client
            .remove(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), create_span!("get"))
            .await;
        let result = self
            .core_kv_client
            .get(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("exists"),
            )
            .await;
        let result = self
            .core_kv_client
            .exists(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("get_and_touch"),
            )
            .await;
        let result = self
            .core_kv_client
            .get_and_touch(id.as_ref(), expiry, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("get_and_lock"),
            )
            .await;
        let result = self
            .core_kv_client
            .get_and_lock(id.as_ref(), lock_time, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("unlock"),
            )
            .await;
        let result = self
            .core_kv_client
            .unlock(id.as_ref(), cas, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("touch"),
            )
            .await;
        let result = self
            .core_kv_client
            .touch(id.as_ref(), expiry, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("lookup_in"),
            )
            .await;
        let result = self
            .core_kv_client
            .lookup_in(id.as_ref(), specs, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("mutate_in"),
            )
            .await;
        let result = self
            .core_kv_client
            .mutate_in(id.as_ref(), specs, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
