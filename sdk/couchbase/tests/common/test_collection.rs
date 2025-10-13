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

use crate::common::helpers::run_with_std_kv_deadline;
use crate::common::node_version::NodeVersion;
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

#[derive(Clone)]
pub struct TestCollection {
    inner: Collection,
    node_version: NodeVersion,
}

impl Deref for TestCollection {
    type Target = Collection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestCollection {
    pub fn new(inner: Collection, node_version: NodeVersion) -> Self {
        Self {
            inner,
            node_version,
        }
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
        run_with_std_kv_deadline(&self.node_version, self.inner.upsert(id, value, options)).await
    }

    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(
            &self.node_version,
            self.inner.upsert_raw(id, value, flags, options),
        )
        .await
    }

    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.insert(id, value, options)).await
    }

    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(
            &self.node_version,
            self.inner.insert_raw(id, value, flags, options),
        )
        .await
    }

    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.replace(id, value, options)).await
    }

    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(
            &self.node_version,
            self.inner.replace_raw(id, value, flags, options),
        )
        .await
    }

    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.remove(id, options)).await
    }

    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> error::Result<GetResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.get(id, options)).await
    }

    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> error::Result<ExistsResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.exists(id, options)).await
    }

    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> error::Result<GetResult> {
        run_with_std_kv_deadline(
            &self.node_version,
            self.inner.get_and_touch(id, expiry, options),
        )
        .await
    }

    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> error::Result<GetResult> {
        run_with_std_kv_deadline(
            &self.node_version,
            self.inner.get_and_lock(id, lock_time, options),
        )
        .await
    }

    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> error::Result<()> {
        run_with_std_kv_deadline(&self.node_version, self.inner.unlock(id, cas, options)).await
    }

    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> error::Result<TouchResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.touch(id, expiry, options)).await
    }

    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> error::Result<LookupInResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.lookup_in(id, specs, options)).await
    }

    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> error::Result<MutateInResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.mutate_in(id, specs, options)).await
    }

    pub fn binary(&self) -> TestBinaryCollection {
        TestBinaryCollection::new(self.inner.binary(), self.node_version.clone())
    }
}
