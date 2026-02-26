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
use crate::tracing::SERVICE_VALUE_KV;
use couchbase_core::create_span;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use tracing::Instrument;

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
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_iter"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let list_contents: Vec<T> = res.content_as()?;
            Ok(list_contents.into_iter())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get<V: DeserializeOwned>(&self, index: usize) -> crate::error::Result<V> {
        let collection = self.collection;
        let id = &self.id;
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_get"),
            )
            .await;
        let result = async move {
            let res = collection
                .lookup_in(id, &[LookupInSpec::get(format!("[{index}]"), None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn remove(&self, index: usize) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_remove"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(
                    &self.id,
                    &[MutateInSpec::remove(format!("[{index}]"), None)],
                    None,
                )
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn append<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_append"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(
                    &self.id,
                    &[MutateInSpec::array_append("", &[value], None)?],
                    MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                )
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn prepend<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_prepend"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(
                    &self.id,
                    &[MutateInSpec::array_prepend("", &[value], None)?],
                    MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                )
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn position<V: PartialEq + DeserializeOwned>(
        &self,
        value: V,
    ) -> crate::error::Result<isize> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_position"),
            )
            .await;
        let result = async {
            let get_res = self.collection.get(&self.id, None).await?;
            let list_contents: Vec<V> = get_res.content_as()?;
            for (i, item) in list_contents.iter().enumerate() {
                if *item == value {
                    return Ok(i as isize);
                }
            }
            Ok(-1)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_len"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("list_clear"),
            )
            .await;
        let result = async {
            self.collection.remove(&self.id, None).await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
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
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_iter"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let list_contents: HashMap<String, T> = res.content_as()?;
            Ok(list_contents.into_iter())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get<V: DeserializeOwned>(&self, id: impl Into<String>) -> crate::error::Result<V> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_get"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::get(id, None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_insert"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(
                    &self.id,
                    &[MutateInSpec::upsert(id, value, None)?],
                    MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                )
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn remove(&self, id: impl Into<String>) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_remove"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(&self.id, &[MutateInSpec::remove(id, None)], None)
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn contains_key(&self, id: impl Into<String>) -> crate::error::Result<bool> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_contains_key"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::exists(id, None)], None)
                .await?;
            res.exists(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_len"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn keys(&self) -> crate::error::Result<Vec<String>> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_keys"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let map_contents: HashMap<String, serde_json::Value> = res.content_as()?;
            Ok(map_contents.keys().cloned().collect())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn values<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_values"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let map_contents: HashMap<String, T> = res.content_as()?;
            Ok(map_contents.into_values().collect())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("map_clear"),
            )
            .await;
        let result = async {
            self.collection.remove(&self.id, None).await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
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
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_iter"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let list_contents: Vec<T> = res.content_as()?;
            Ok(list_contents.into_iter())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn insert<V: Serialize>(&self, value: V) -> crate::error::Result<bool> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_insert"),
            )
            .await;
        let result = async {
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
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn remove<T: DeserializeOwned + PartialEq>(
        &self,
        value: T,
    ) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_remove"),
            )
            .await;
        let result = async {
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
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn values<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_values"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let set_contents: Vec<T> = res.content_as()?;
            Ok(set_contents)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn contains<T: PartialEq + DeserializeOwned>(
        &self,
        value: T,
    ) -> crate::error::Result<bool> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_contains"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let set_contents: Vec<T> = res.content_as()?;
            for item in set_contents {
                if item == value {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_len"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("set_clear"),
            )
            .await;
        let result = async {
            self.collection.remove(&self.id, None).await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
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
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("queue_iter"),
            )
            .await;
        let result = async {
            let res = self.collection.get(&self.id, None).await?;
            let mut list_contents: Vec<T> = res.content_as()?;
            list_contents.reverse();
            Ok(list_contents.into_iter())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn push<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("queue_push"),
            )
            .await;
        let result = async {
            self.collection
                .mutate_in(
                    &self.id,
                    &[MutateInSpec::array_prepend("", &[value], None)?],
                    MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
                )
                .await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn pop<T: DeserializeOwned>(&self) -> crate::error::Result<T> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("queue_pop"),
            )
            .await;
        let result = async {
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
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn len(&self) -> crate::error::Result<usize> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("queue_len"),
            )
            .await;
        let result = async {
            let res = self
                .collection
                .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
                .await?;
            res.content_as(0)
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn clear(&self) -> crate::error::Result<()> {
        let ctx = self
            .collection
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.collection.keyspace(),
                create_span!("queue_clear"),
            )
            .await;
        let result = async {
            self.collection.remove(&self.id, None).await?;
            Ok(())
        }
        .instrument(ctx.span().clone())
        .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
