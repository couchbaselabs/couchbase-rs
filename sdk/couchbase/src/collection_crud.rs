use crate::collection::Collection;
use crate::options::kv_options::*;
use crate::results::kv_results::*;
use crate::transcoding;
use crate::transcoding::RawValue;
use serde::Serialize;
use std::time::Duration;

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.upsert_raw(id.into(), transcoding::json::encode(value)?, options)
            .await
    }

    pub async fn upsert_raw(
        &self,
        id: impl Into<String>,
        value: RawValue,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .upsert(id.into(), value.content, value.flags, options)
            .await
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.insert_raw(id, transcoding::json::encode(value)?, options)
            .await
    }

    pub async fn insert_raw(
        &self,
        id: impl Into<String>,
        value: RawValue,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .insert(id.into(), value.content, value.flags, options)
            .await
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl Into<String>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        self.replace_raw(id, transcoding::json::encode(value)?, options)
            .await
    }

    pub async fn replace_raw(
        &self,
        id: impl Into<String>,
        value: RawValue,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .replace(id.into(), value.content, value.flags, options)
            .await
    }

    pub async fn remove(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.remove(id.into(), options).await
    }

    pub async fn get(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.get(id.into(), options).await
    }

    pub async fn exists(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.exists(id.into(), options).await
    }

    pub async fn get_and_touch(
        &self,
        id: impl Into<String>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .get_and_touch(id.into(), expiry, options)
            .await
    }

    pub async fn get_and_lock(
        &self,
        id: impl Into<String>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .get_and_lock(id.into(), lock_time, options)
            .await
    }

    pub async fn unlock(
        &self,
        id: impl Into<String>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.unlock(id.into(), cas, options).await
    }

    pub async fn touch(
        &self,
        id: impl Into<String>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.touch(id.into(), expiry, options).await
    }
}
