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
use couchbase::collection::BinaryCollection;
use couchbase::error;
use couchbase::options::kv_binary_options::*;
use couchbase::results::kv_binary_results::CounterResult;
use couchbase::results::kv_results::*;
use std::ops::Deref;

#[derive(Clone)]
pub struct TestBinaryCollection {
    inner: BinaryCollection,
    node_version: NodeVersion,
}

impl Deref for TestBinaryCollection {
    type Target = BinaryCollection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestBinaryCollection {
    pub fn new(inner: BinaryCollection, node_version: NodeVersion) -> Self {
        Self {
            inner,
            node_version,
        }
    }

    pub async fn append(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<AppendOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.append(id, value, options)).await
    }

    pub async fn prepend(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        options: impl Into<Option<PrependOptions>>,
    ) -> error::Result<MutationResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.prepend(id, value, options)).await
    }

    pub async fn increment(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<IncrementOptions>>,
    ) -> error::Result<CounterResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.increment(id, options)).await
    }

    pub async fn decrement(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<DecrementOptions>>,
    ) -> error::Result<CounterResult> {
        run_with_std_kv_deadline(&self.node_version, self.inner.decrement(id, options)).await
    }
}
