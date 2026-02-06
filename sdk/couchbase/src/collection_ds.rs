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
use crate::options::collection_ds_options::{
    CouchbaseListOptions, CouchbaseMapOptions, CouchbaseQueueOptions, CouchbaseSetOptions,
};
use crate::options::kv_options::{MutateInOptions, StoreSemantics};
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use crate::tracing::{
    SERVICE_VALUE_KV, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct CouchbaseList<'a> {
    pub collection: &'a Collection,
    pub id: String,
    pub options: CouchbaseListOptions,
}

impl Collection {
    pub fn list(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<CouchbaseListOptions>>,
    ) -> CouchbaseList<'_> {
        CouchbaseList {
            collection: self,
            id: id.into(),
            options: options.into().unwrap_or_default(),
        }
    }

    pub fn map(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<CouchbaseMapOptions>>,
    ) -> CouchbaseMap<'_> {
        CouchbaseMap {
            collection: self,
            id: id.into(),
            options: options.into().unwrap_or_default(),
        }
    }

    pub fn set(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<CouchbaseSetOptions>>,
    ) -> CouchbaseSet<'_> {
        CouchbaseSet {
            collection: self,
            id: id.into(),
            options: options.into().unwrap_or_default(),
        }
    }

    pub fn queue(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<CouchbaseQueueOptions>>,
    ) -> CouchbaseQueue<'_> {
        CouchbaseQueue {
            collection: self,
            id: id.into(),
            options: options.into().unwrap_or_default(),
        }
    }
}

impl CouchbaseList<'_> {
    pub async fn iter<T: DeserializeOwned>(&self) -> crate::error::Result<impl Iterator<Item = T>> {
        self.iter_internal().await
    }

    pub async fn get<V: DeserializeOwned>(&self, index: usize) -> crate::error::Result<V> {
        self.get_internal(index).await
    }

    pub async fn remove(&self, index: usize) -> crate::error::Result<()> {
        self.remove_internal(index).await
    }

    pub async fn append<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.append_internal(value).await
    }

    pub async fn prepend<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.prepend_internal(value).await
    }

    pub async fn position<V: PartialEq + DeserializeOwned>(
        &self,
        value: V,
    ) -> crate::error::Result<isize> {
        self.position_internal(value).await
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        self.len_internal().await
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        self.clear_internal().await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_iter",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_iter",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_iter",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;
                    let list_contents: Vec<T> = res.content_as()?;

                    Ok(list_contents.into_iter())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_get",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_get",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_internal<V: DeserializeOwned>(&self, index: usize) -> crate::error::Result<V> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_get",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(
                            &self.id,
                            &[LookupInSpec::get(format!("[{index}]"), None)],
                            None,
                        )
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_remove",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_remove",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn remove_internal(&self, index: usize) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_remove",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::remove(format!("[{index}]"), None)],
                            None,
                        )
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_append",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_append",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn append_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_append",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::array_append("", &[value], None)?],
                            MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                        )
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_prepend",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_prepend",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn prepend_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_prepend",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::array_prepend("", &[value], None)?],
                            MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                        )
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_position",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_position",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn position_internal<V: PartialEq + DeserializeOwned>(
        &self,
        value: V,
    ) -> crate::error::Result<isize> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_position",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let get_res = self.collection.get(&self.id, None).await?;

                    let list_contents: Vec<V> = get_res.content_as()?;
                    for (i, item) in list_contents.iter().enumerate() {
                        if *item == value {
                            return Ok(i as isize);
                        }
                    }

                    Ok(-1)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_len",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_len",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_len",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_clear",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "list_clear",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "list_clear",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection.remove(&self.id, None).await?;
                    Ok(())
                },
            )
            .await
    }
}

#[derive(Clone)]
pub struct CouchbaseMap<'a> {
    pub collection: &'a Collection,
    pub id: String,
    pub options: CouchbaseMapOptions,
}

impl CouchbaseMap<'_> {
    pub async fn iter<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = (String, T)>> {
        self.iter_internal().await
    }

    pub async fn get<V: DeserializeOwned>(&self, id: impl Into<String>) -> crate::error::Result<V> {
        self.get_internal(id).await
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        self.insert_internal(id, value).await
    }

    pub async fn remove(&self, id: impl Into<String>) -> crate::error::Result<()> {
        self.remove_internal(id).await
    }

    pub async fn contains_key(&self, id: impl Into<String>) -> crate::error::Result<bool> {
        self.contains_key_internal(id).await
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        self.len_internal().await
    }

    pub async fn keys(&self) -> crate::error::Result<Vec<String>> {
        self.keys_internal().await
    }

    pub async fn values<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.values_internal().await
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        self.clear_internal().await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_iter",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_iter",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = (String, T)>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_iter",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;
                    let list_contents: HashMap<String, T> = res.content_as()?;

                    Ok(list_contents.into_iter())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_get",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_get",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn get_internal<V: DeserializeOwned>(
        &self,
        id: impl Into<String>,
    ) -> crate::error::Result<V> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_get",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::get(id, None)], None)
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_insert",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_insert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn insert_internal<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_insert",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::upsert(id, value, None)?],
                            MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                        )
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_remove",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_remove",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn remove_internal(&self, id: impl Into<String>) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_remove",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(&self.id, &[MutateInSpec::remove(id, None)], None)
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_contains_key",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_contains_key",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn contains_key_internal(&self, id: impl Into<String>) -> crate::error::Result<bool> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_contains_key",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::exists(id, None)], None)
                        .await?;

                    res.exists(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_len",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_len",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_len",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_keys",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_keys",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn keys_internal(&self) -> crate::error::Result<Vec<String>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_keys",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;

                    let map_contents: HashMap<String, serde_json::Value> = res.content_as()?;
                    Ok(map_contents.keys().cloned().collect())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_values",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_values",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn values_internal<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_values",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;

                    let map_contents: HashMap<String, T> = res.content_as()?;
                    Ok(map_contents.into_values().collect())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_clear",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "map_clear",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "map_clear",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection.remove(&self.id, None).await?;
                    Ok(())
                },
            )
            .await
    }
}

#[derive(Clone)]
pub struct CouchbaseSet<'a> {
    pub collection: &'a Collection,
    pub id: String,
    pub options: CouchbaseSetOptions,
}

impl CouchbaseSet<'_> {
    pub async fn iter<T: DeserializeOwned>(&self) -> crate::error::Result<impl Iterator<Item = T>> {
        self.iter_internal().await
    }

    pub async fn insert<V: Serialize>(&self, value: V) -> crate::error::Result<(bool)> {
        self.insert_internal(value).await
    }

    pub async fn remove<T: DeserializeOwned + PartialEq>(
        &self,
        value: T,
    ) -> crate::error::Result<()> {
        self.remove_internal(value).await
    }

    pub async fn values<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.values_internal().await
    }

    pub async fn contains<T: PartialEq + DeserializeOwned>(
        &self,
        value: T,
    ) -> crate::error::Result<bool> {
        self.contains_internal(value).await
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        self.len_internal().await
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        self.clear_internal().await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_iter",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_iter",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_iter",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;
                    let list_contents: Vec<T> = res.content_as()?;

                    Ok(list_contents.into_iter())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_insert",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_insert",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn insert_internal<V: Serialize>(&self, value: V) -> crate::error::Result<(bool)> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_insert",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::array_add_unique("", value, None)?],
                            MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                        )
                        .await;

                    if let Err(e) = res {
                        return match e.kind() {
                            crate::error::ErrorKind::PathExists => Ok(false),
                            _ => Err(e),
                        };
                    }

                    Ok(true)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_remove",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_remove",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn remove_internal<T: DeserializeOwned + PartialEq>(
        &self,
        value: T,
    ) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_remove",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    for _ in 0..16 {
                        let items = self.collection.get(&self.id, None).await?;
                        let cas = items.cas();

                        let set_contents: Vec<T> = items.content_as()?;

                        let mut index_to_remove: Option<usize> = None;
                        for (i, item) in set_contents.iter().enumerate() {
                            if *item == value {
                                index_to_remove = Some(i);
                            }
                        }
                        if let Some(index) = index_to_remove {
                            let res = self
                                .collection
                                .mutate_in(
                                    &self.id,
                                    &[MutateInSpec::remove(format!("[{index}]"), None)],
                                    MutateInOptions::new().cas(cas),
                                )
                                .await;
                            if let Err(e) = res {
                                match e.kind() {
                                    crate::error::ErrorKind::DocumentExists => continue,
                                    crate::error::ErrorKind::CasMismatch => continue,
                                    _ => return Err(e),
                                };
                            }
                        }
                        return Ok(());
                    }

                    Err(crate::error::Error::other_failure(
                        "failed to perform operation after 16 retries",
                    ))
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_values",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_values",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn values_internal<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_values",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;

                    let set_contents: Vec<T> = res.content_as()?;
                    Ok(set_contents)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_contains",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_contains",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn contains_internal<T: PartialEq + DeserializeOwned>(
        &self,
        value: T,
    ) -> crate::error::Result<bool> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_contains",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;

                    let set_contents: Vec<T> = res.content_as()?;

                    for item in set_contents {
                        if item == value {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_len",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_len",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_len",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_clear",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "set_clear",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "set_clear",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection.remove(&self.id, None).await?;
                    Ok(())
                },
            )
            .await
    }
}

#[derive(Clone)]
pub struct CouchbaseQueue<'a> {
    pub collection: &'a Collection,
    pub id: String,
    pub options: CouchbaseQueueOptions,
}

impl CouchbaseQueue<'_> {
    pub async fn iter<T: DeserializeOwned>(&self) -> crate::error::Result<impl Iterator<Item = T>> {
        self.iter_internal().await
    }

    pub async fn push<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.push_internal(value).await
    }

    pub async fn pop<T: DeserializeOwned>(&self) -> crate::error::Result<T> {
        self.pop_internal().await
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        self.len_internal().await
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        self.clear_internal().await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_iter",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "queue_iter",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "queue_iter",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self.collection.get(&self.id, None).await?;

                    let mut list_contents: Vec<T> = res.content_as()?;
                    list_contents.reverse();

                    Ok(list_contents.into_iter())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_push",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "queue_push",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn push_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "queue_push",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection
                        .mutate_in(
                            &self.id,
                            &[MutateInSpec::array_prepend("", &[value], None)?],
                            MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                        )
                        .await?;

                    Ok(())
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_pop",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "queue_pop",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn pop_internal<T: DeserializeOwned>(&self) -> crate::error::Result<T> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "queue_pop",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    for _ in 0..16 {
                        let res = self
                            .collection
                            .lookup_in(&self.id, &[LookupInSpec::get("[-1]", None)], None)
                            .await?;
                        let cas = res.cas();
                        let value: T = res.content_as(0)?;

                        let res = self
                            .collection
                            .mutate_in(
                                &self.id,
                                &[MutateInSpec::remove("[-1]", None)],
                                MutateInOptions::new().cas(cas),
                            )
                            .await;
                        if let Err(e) = res {
                            match e.kind() {
                                crate::error::ErrorKind::DocumentExists => continue,
                                crate::error::ErrorKind::CasMismatch => continue,
                                _ => return Err(e),
                            };
                        }
                        return Ok(value);
                    }

                    Err(crate::error::Error::other_failure(
                        "failed to perform operation after 16 retries",
                    ))
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_len",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "queue_len",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "queue_len",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    let res = self
                        .collection
                        .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                        .await?;

                    res.content_as(0)
                },
            )
            .await
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_clear",
        fields(
        otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
        db.operation.name = "queue_clear",
        db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
        db.namespace = self.collection.bucket_name(),
        couchbase.scope.name = self.collection.scope_name(),
        couchbase.collection.name = self.collection.name(),
        couchbase.service = SERVICE_VALUE_KV,
        couchbase.retries = 0,
        couchbase.cluster.name,
        couchbase.cluster.uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection
            .tracing_client
            .execute_metered_operation(
                "queue_clear",
                Some(SERVICE_VALUE_KV),
                &self.collection.keyspace,
                async move {
                    self.collection.tracing_client.record_generic_fields().await;
                    self.collection.remove(&self.id, None).await?;
                    Ok(())
                },
            )
            .await
    }
}
