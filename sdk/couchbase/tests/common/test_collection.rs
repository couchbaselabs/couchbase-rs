use crate::common::test_binary_collection::TestBinaryCollection;
use crate::common::test_query_index_manager::TestQueryIndexManager;
use couchbase::collection::Collection;
use couchbase::error;
use couchbase::options::kv_options::*;
use couchbase::results::kv_results::*;
use couchbase::subdoc::lookup_in_specs::LookupInSpec;
use couchbase::subdoc::mutate_in_specs::MutateInSpec;
use serde::Serialize;
use std::ops::Deref;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct TestCollection {
    inner: Collection,
}

impl Deref for TestCollection {
    type Target = Collection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestCollection {
    pub fn new(inner: Collection) -> Self {
        Self { inner }
    }

    pub fn query_indexes(&self) -> TestQueryIndexManager {
        TestQueryIndexManager::new(self.inner.query_indexes())
    }

    pub async fn upsert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.upsert(id, value, options),
        )
        .await
        .unwrap()
    }

    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.upsert_raw(id, value, flags, options),
        )
        .await
        .unwrap()
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.insert(id, value, options),
        )
        .await
        .unwrap()
    }

    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.insert_raw(id, value, flags, options),
        )
        .await
        .unwrap()
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.replace(id, value, options),
        )
        .await
        .unwrap()
    }

    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.replace_raw(id, value, flags, options),
        )
        .await
        .unwrap()
    }

    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> error::Result<MutationResult> {
        timeout(Duration::from_millis(2500), self.inner.remove(id, options))
            .await
            .unwrap()
    }

    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> error::Result<GetResult> {
        timeout(Duration::from_millis(2500), self.inner.get(id, options))
            .await
            .unwrap()
    }

    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> error::Result<ExistsResult> {
        timeout(Duration::from_millis(2500), self.inner.exists(id, options))
            .await
            .unwrap()
    }

    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> error::Result<GetResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.get_and_touch(id, expiry, options),
        )
        .await
        .unwrap()
    }

    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> error::Result<GetResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.get_and_lock(id, lock_time, options),
        )
        .await
        .unwrap()
    }

    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_millis(2500),
            self.inner.unlock(id, cas, options),
        )
        .await
        .unwrap()
    }

    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> error::Result<TouchResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.touch(id, expiry, options),
        )
        .await
        .unwrap()
    }

    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> error::Result<LookupInResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.lookup_in(id, specs, options),
        )
        .await
        .unwrap()
    }

    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> error::Result<MutateInResult> {
        timeout(
            Duration::from_millis(2500),
            self.inner.mutate_in(id, specs, options),
        )
        .await
        .unwrap()
    }

    pub fn binary(&self) -> TestBinaryCollection {
        TestBinaryCollection::new(self.inner.binary())
    }
}
