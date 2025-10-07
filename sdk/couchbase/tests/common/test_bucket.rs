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

use crate::common::test_collection::TestCollection;
use crate::common::test_manager::TestCollectionManager;
use crate::common::test_scope::TestScope;
use couchbase::bucket::Bucket;
use couchbase::error;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct TestBucket {
    inner: Bucket,
}

impl std::ops::Deref for TestBucket {
    type Target = Bucket;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestBucket {
    pub fn new(inner: Bucket) -> Self {
        Self { inner }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn scope(&self, name: impl Into<String>) -> TestScope {
        TestScope::new(self.inner.scope(name))
    }

    pub fn collection(&self, name: impl Into<String>) -> TestCollection {
        TestCollection::new(self.inner.collection(name))
    }

    pub fn default_collection(&self) -> TestCollection {
        TestCollection::new(self.inner.default_collection())
    }

    pub fn collections(&self) -> TestCollectionManager {
        TestCollectionManager::new(self.inner.collections())
    }

    pub async fn ping(
        &self,
        opts: impl Into<Option<couchbase::options::diagnostic_options::PingOptions>>,
    ) -> error::Result<couchbase::results::diagnostics::PingReport> {
        timeout(Duration::from_secs(15), self.inner.ping(opts))
            .await
            .unwrap()
    }

    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<couchbase::options::diagnostic_options::WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        timeout(Duration::from_secs(15), self.inner.wait_until_ready(opts))
            .await
            .unwrap()
    }
}
