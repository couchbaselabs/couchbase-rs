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

use crate::collectionresolver::CollectionResolver;
use crate::error::{Error, Result};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{
    orchestrate_random_memd_client, KvClientManager, KvClientManagerClientType,
};
use crate::memdx::request::GetCollectionIdRequest;
use crate::memdx::response::GetCollectionIdResponse;
use futures::TryFutureExt;
use std::sync::Arc;

pub(crate) struct CollectionResolverMemd<K> {
    conn_mgr: Arc<K>,
}

pub(crate) struct CollectionResolverMemdOptions<K> {
    pub conn_mgr: Arc<K>,
}

impl<K> CollectionResolverMemd<K>
where
    K: KvClientManager,
{
    pub fn new(opts: CollectionResolverMemdOptions<K>) -> Self {
        Self {
            conn_mgr: opts.conn_mgr,
        }
    }
}

impl<K> CollectionResolver for CollectionResolverMemd<K>
where
    K: KvClientManager,
{
    async fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> Result<(u32, u64)> {
        let resp: GetCollectionIdResponse = orchestrate_random_memd_client(
            self.conn_mgr.clone(),
            async |client: Arc<KvClientManagerClientType<K>>| {
                client
                    .get_collection_id(GetCollectionIdRequest {
                        scope_name,
                        collection_name,
                    })
                    .map_err(Error::new_contextual_memdx_error)
                    .await
            },
        )
        .await?;

        Ok((resp.collection_id, resp.manifest_rev))
    }

    async fn invalidate_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
        endpoint: &str,
        manifest_rev: u64,
    ) {
    }
}
