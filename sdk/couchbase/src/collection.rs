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
use crate::clients::collection_client::CollectionClient;
use crate::clients::core_kv_client::CoreKvClient;
use crate::clients::query_index_mgmt_client::QueryIndexMgmtClient;
use crate::clients::tracing_client::TracingClient;
use crate::management::query::query_index_manager::QueryIndexManager;
use crate::tracing::Keyspace;
use std::sync::Arc;

#[derive(Clone)]
pub struct Collection {
    pub(crate) client: CollectionClient,
    pub(crate) core_kv_client: CoreKvClient,
    pub(crate) query_index_management_client: Arc<QueryIndexMgmtClient>,
    pub(crate) tracing_client: Arc<TracingClient>,
    pub(crate) bucket_name: String,
    pub(crate) scope_name: String,
    pub(crate) collection_name: String,
}

impl Collection {
    pub(crate) fn new(client: CollectionClient) -> Self {
        let core_kv_client = client.core_kv_client();
        let query_index_management_client = Arc::new(client.query_index_management_client());
        let tracing_client = Arc::new(client.tracing_client());
        let bucket_name = client.bucket_name().to_string();
        let scope_name = client.scope_name().to_string();
        let collection_name = client.name().to_string();
        Self {
            client,
            core_kv_client,
            query_index_management_client,
            tracing_client,
            bucket_name,
            scope_name,
            collection_name,
        }
    }

    pub(crate) fn keyspace(&self) -> Keyspace<'_> {
        Keyspace::Collection {
            bucket: &self.bucket_name,
            scope: &self.scope_name,
            collection: &self.collection_name,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn scope_name(&self) -> &str {
        self.client.scope_name()
    }

    pub fn bucket_name(&self) -> &str {
        self.client.bucket_name()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(
            self.core_kv_client.clone(),
            self.tracing_client.clone(),
            self.bucket_name.clone(),
            self.scope_name.clone(),
            self.collection_name.clone(),
        )
    }

    pub fn query_indexes(&self) -> QueryIndexManager {
        QueryIndexManager {
            client: self.query_index_management_client.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BinaryCollection {
    pub(crate) core_kv_client: CoreKvClient,
    pub(crate) tracing_client: Arc<TracingClient>,
    pub(crate) bucket_name: String,
    pub(crate) scope_name: String,
    pub(crate) collection_name: String,
}

impl BinaryCollection {
    pub(crate) fn new(
        core_kv_client: CoreKvClient,
        tracing_client: Arc<TracingClient>,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
    ) -> Self {
        Self {
            core_kv_client,
            tracing_client,
            bucket_name,
            scope_name,
            collection_name,
        }
    }

    pub(crate) fn keyspace(&self) -> Keyspace<'_> {
        Keyspace::Collection {
            bucket: &self.bucket_name,
            scope: &self.scope_name,
            collection: &self.collection_name,
        }
    }
}
