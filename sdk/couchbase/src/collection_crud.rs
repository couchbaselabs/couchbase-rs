use crate::collection::Collection;
use crate::options::kv_options::*;
use crate::results::kv_results::*;
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use crate::transcoding;
use serde::Serialize;
use std::time::Duration;

impl Collection {
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let (value, flags) = transcoding::json::encode(value)?;
        self.upsert_raw(id, &value, flags, options).await
    }

    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .upsert(id.as_ref(), value, flags, options)
            .await
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let (value, flags) = transcoding::json::encode(value)?;
        self.insert_raw(id, &value, flags, options).await
    }

    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .insert(id.as_ref(), value, flags, options)
            .await
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let (value, flags) = transcoding::json::encode(value)?;
        self.replace_raw(id, &value, flags, options).await
    }

    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .replace(id.as_ref(), value, flags, options)
            .await
    }

    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.remove(id.as_ref(), options).await
    }

    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.get(id.as_ref(), options).await
    }

    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.exists(id.as_ref(), options).await
    }

    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .get_and_touch(id.as_ref(), expiry, options)
            .await
    }

    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .get_and_lock(id.as_ref(), lock_time, options)
            .await
    }

    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client.unlock(id.as_ref(), cas, options).await
    }

    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .touch(id.as_ref(), expiry, options)
            .await
    }

    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .lookup_in(id.as_ref(), specs, options)
            .await
    }

    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        let options = options.into().unwrap_or_default();
        self.core_kv_client
            .mutate_in(id.as_ref(), specs, options)
            .await
    }
}
