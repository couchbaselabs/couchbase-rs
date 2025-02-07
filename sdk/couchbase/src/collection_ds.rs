use crate::collection::Collection;
use crate::options::collection_ds_options::{
    CouchbaseListOptions, CouchbaseMapOptions, CouchbaseQueueOptions, CouchbaseSetOptions,
};
use crate::options::kv_options::{MutateInOptions, StoreSemantics};
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
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
    ) -> CouchbaseList {
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
    ) -> CouchbaseMap {
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
    ) -> CouchbaseSet {
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
    ) -> CouchbaseQueue {
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
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_iter",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;
        let list_contents: Vec<T> = res.content_as()?;

        Ok(list_contents.into_iter())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_get",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_get",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn get_internal<V: DeserializeOwned>(&self, index: usize) -> crate::error::Result<V> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(
                &self.id,
                &[LookupInSpec::get(format!("[{}]", index), None)],
                None,
            )
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_remove",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_remove",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn remove_internal(&self, index: usize) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(
                &self.id,
                &[MutateInSpec::remove(format!("[{}]", index), None)],
                None,
            )
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_append",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_append",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn append_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(
                &self.id,
                &[MutateInSpec::array_append("", &[value], None)?],
                MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
            )
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_prepend",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_prepend",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn prepend_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(
                &self.id,
                &[MutateInSpec::array_prepend("", &[value], None)?],
                MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
            )
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_position",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_position",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn position_internal<V: PartialEq + DeserializeOwned>(
        &self,
        value: V,
    ) -> crate::error::Result<isize> {
        self.collection.tracing_client.record_generic_fields().await;

        let get_res = self.collection.get(&self.id, None).await?;

        let list_contents: Vec<V> = get_res.content_as()?;
        for (i, item) in list_contents.iter().enumerate() {
            if *item == value {
                return Ok(i as isize);
            }
        }

        Ok(-1)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_len",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_len",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "list_clear",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "list_clear",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    pub async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection.remove(&self.id, None).await?;
        Ok(())
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
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_iter",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = (String, T)>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;
        let list_contents: HashMap<String, T> = res.content_as()?;

        Ok(list_contents.into_iter())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_get",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_get",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn get_internal<V: DeserializeOwned>(
        &self,
        id: impl Into<String>,
    ) -> crate::error::Result<V> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::get(id, None)], None)
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_insert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_insert",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn insert_internal<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
    ) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(
                &self.id,
                &[MutateInSpec::upsert(id, value, None)?],
                MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
            )
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_remove",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_remove",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn remove_internal(&self, id: impl Into<String>) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(&self.id, &[MutateInSpec::remove(id, None)], None)
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_contains_key",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_contains_key",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn contains_key_internal(&self, id: impl Into<String>) -> crate::error::Result<bool> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::exists(id, None)], None)
            .await?;

        res.exists(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_len",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_len",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_keys",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_keys",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn keys_internal(&self) -> crate::error::Result<Vec<String>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;

        let map_contents: HashMap<String, serde_json::Value> = res.content_as()?;
        Ok(map_contents.keys().cloned().collect())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_values",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_values",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn values_internal<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;

        let map_contents: HashMap<String, T> = res.content_as()?;
        Ok(map_contents.into_values().collect())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "map_clear",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "map_clear",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection.remove(&self.id, None).await?;
        Ok(())
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
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_iter",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;
        let list_contents: Vec<T> = res.content_as()?;

        Ok(list_contents.into_iter())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_insert",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_insert",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn insert_internal<V: Serialize>(&self, value: V) -> crate::error::Result<(bool)> {
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
            // TODO Check for errors properly once we have proper error types
            return if e.msg.contains("subdoc path exists") {
                Ok(false)
            } else {
                Err(e)
            };
        }

        Ok(true)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_remove",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_remove",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn remove_internal<T: DeserializeOwned + PartialEq>(
        &self,
        value: T,
    ) -> crate::error::Result<()> {
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
                        &[MutateInSpec::remove(format!("[{}]", index), None)],
                        MutateInOptions::new().cas(cas),
                    )
                    .await;
                if let Err(e) = res {
                    // TODO Check for errors properly once we have proper error types
                    if e.msg.contains("CAS mismatch") || e.msg.contains("Key exists") {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
            return Ok(());
        }

        Err(crate::error::Error {
            msg: "failed to perform operation after 16 retries".into(),
        })
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_values",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_values",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn values_internal<T: DeserializeOwned>(&self) -> crate::error::Result<Vec<T>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;

        let set_contents: Vec<T> = res.content_as()?;
        Ok(set_contents)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_contains",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_contains",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn contains_internal<T: PartialEq + DeserializeOwned>(
        &self,
        value: T,
    ) -> crate::error::Result<bool> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;

        let set_contents: Vec<T> = res.content_as()?;

        for item in set_contents {
            if item == value {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_len",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_len",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "set_clear",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "set_clear",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection.remove(&self.id, None).await?;
        Ok(())
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
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "queue_iter",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn iter_internal<T: DeserializeOwned>(
        &self,
    ) -> crate::error::Result<impl Iterator<Item = T>> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self.collection.get(&self.id, None).await?;

        let mut list_contents: Vec<T> = res.content_as()?;
        list_contents.reverse();

        Ok(list_contents.into_iter())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_push",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "queue_push",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn push_internal<V: Serialize>(&self, value: V) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection
            .mutate_in(
                &self.id,
                &[MutateInSpec::array_prepend("", &[value], None)?],
                MutateInOptions::new().store_semantics(StoreSemantics::Upsert),
            )
            .await?;

        Ok(())
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_pop",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "queue_pop",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn pop_internal<T: DeserializeOwned>(&self) -> crate::error::Result<T> {
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
                // TODO Check for errors properly once we have proper error types
                if e.msg.contains("CAS mismatch") || e.msg.contains("Key exists") {
                    continue;
                } else {
                    return Err(e);
                }
            }
            return Ok(value);
        }

        Err(crate::error::Error {
            msg: "failed to perform operation after 16 retries".into(),
        })
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_len",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "queue_len",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn len_internal(&self) -> crate::error::Result<usize> {
        self.collection.tracing_client.record_generic_fields().await;

        let res = self
            .collection
            .lookup_in(&self.id, &[LookupInSpec::count("", None)], None)
            .await?;

        res.content_as(0)
    }

    #[instrument(
        skip_all,
        level = Level::TRACE,
        name = "queue_clear",
        fields(
        otel.kind = "client",
        db.system = "couchbase",
        db.couchbase.service = "kv",
        db.operation = "queue_clear",
        db.name = self.collection.bucket_name(),
        db.couchbase.collection = self.collection.name(),
        db.couchbase.scope = self.collection.scope_name(),
        db.couchbase.retries = 0,
        db.couchbase.cluster_name,
        db.couchbase.cluster_uuid,
        ))]
    async fn clear_internal(&self) -> crate::error::Result<()> {
        self.collection.tracing_client.record_generic_fields().await;

        self.collection.remove(&self.id, None).await?;
        Ok(())
    }
}
