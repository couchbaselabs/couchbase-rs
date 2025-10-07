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
use crate::management::query::query_index_manager::QueryIndexManager;
use std::sync::Arc;

#[derive(Clone)]
pub struct Collection {
    pub(crate) client: CollectionClient,
    pub(crate) core_kv_client: CoreKvClient,
    pub(crate) query_index_management_client: Arc<QueryIndexMgmtClient>,
}

impl Collection {
    pub(crate) fn new(client: CollectionClient) -> Self {
        let core_kv_client = client.core_kv_client();
        let query_index_management_client = Arc::new(client.query_index_management_client());
        Self {
            client,
            core_kv_client,
            query_index_management_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(self.core_kv_client.clone())
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
}

impl BinaryCollection {
    pub(crate) fn new(core_kv_client: CoreKvClient) -> Self {
        Self { core_kv_client }
    }
}
