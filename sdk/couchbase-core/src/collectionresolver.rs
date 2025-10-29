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

use crate::error::{Error, Result};
use crate::memdx::error::ServerErrorKind;
use std::future::Future;
use std::sync::Arc;

pub(crate) trait CollectionResolver: Sized + Send + Sync {
    fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> impl Future<Output = Result<(u32, u64)>> + Send;

    fn invalidate_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) async fn orchestrate_memd_collection_id<Cr, Resp, Fut>(
    resolver: Arc<Cr>,
    scope_name: &str,
    collection_name: &str,
    mut operation: impl Fn(u32) -> Fut,
) -> Result<Resp>
where
    Cr: CollectionResolver,
    Fut: Future<Output = Result<Resp>> + Send,
{
    if (scope_name == "_default" && collection_name == "_default")
        || (scope_name.is_empty() && collection_name.is_empty())
    {
        // We never want to invalidate this id.
        return operation(0).await;
    }

    let (collection_id, _manifest_rev) = match resolver
        .resolve_collection_id(scope_name, collection_name)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return Err(invalidate_collection_id(resolver, e, scope_name, collection_name).await);
        }
    };

    let err = match operation(collection_id).await {
        Ok(r) => return Ok(r),
        Err(e) => e,
    };

    Err(invalidate_collection_id(resolver, err, scope_name, collection_name).await)
}

async fn invalidate_collection_id<Cr>(
    resolver: Arc<Cr>,
    err: Error,
    scope_name: &str,
    collection_name: &str,
) -> Error
where
    Cr: CollectionResolver,
{
    if let Some(memdx_err) = err.is_memdx_error() {
        if memdx_err.is_server_error_kind(ServerErrorKind::UnknownCollectionID)
            || memdx_err.is_server_error_kind(ServerErrorKind::UnknownCollectionName)
        {
            resolver
                .invalidate_collection_id(scope_name, collection_name)
                .await;
        }
    }

    err
}
