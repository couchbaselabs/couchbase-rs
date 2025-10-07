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

use couchbase::error;
use couchbase::management::query::query_index_manager::QueryIndexManager;
use couchbase::options::query_index_mgmt_options::*;
use couchbase::results::query_index_mgmt_results::*;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct TestQueryIndexManager {
    inner: QueryIndexManager,
}

impl std::ops::Deref for TestQueryIndexManager {
    type Target = QueryIndexManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestQueryIndexManager {
    pub fn new(inner: QueryIndexManager) -> Self {
        Self { inner }
    }

    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllQueryIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        timeout(Duration::from_secs(20), self.inner.get_all_indexes(opts))
            .await
            .unwrap()
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.create_index(index_name, fields, opts),
        )
        .await
        .unwrap()
    }

    pub async fn create_primary_index(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.create_primary_index(opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_index(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_primary_index(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(Duration::from_secs(20), self.inner.drop_primary_index(opts))
            .await
            .unwrap()
    }

    pub async fn watch_indexes(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.watch_indexes(index_names, opts),
        )
        .await
        .unwrap()
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.build_deferred_indexes(opts),
        )
        .await
        .unwrap()
    }
}
