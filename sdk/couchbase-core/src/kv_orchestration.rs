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
use crate::error;
use crate::kvendpointclientmanager::KvEndpointClientManager;
use std::future::Future;
use std::sync::Arc;

pub(crate) type KvClientManagerClientType<M> = <M as KvEndpointClientManager>::Client;

pub(crate) async fn orchestrate_kv_client<Resp, M, Fut>(
    manager: Arc<M>,
    mut operation: impl FnMut(Arc<KvClientManagerClientType<M>>) -> Fut,
) -> error::Result<Resp>
where
    M: KvEndpointClientManager,
    Fut: Future<Output = error::Result<Resp>> + Send,
{
    loop {
        let client = manager.get_client().await?;

        let res = operation(client.clone()).await;
        return match res {
            Ok(r) => Ok(r),
            Err(e) => {
                if let Some(memdx_err) = e.is_memdx_error() {
                    if memdx_err.is_dispatch_error() {
                        // this was a dispatch error, so we can just try with
                        // a different client instead...
                        continue;
                    }
                }

                Err(e)
            }
        };
    }
}

pub(crate) async fn orchestrate_endpoint_kv_client<Resp, M, Fut>(
    manager: Arc<M>,
    endpoint: &str,
    mut operation: impl FnMut(Arc<KvClientManagerClientType<M>>) -> Fut,
) -> error::Result<Resp>
where
    M: KvEndpointClientManager,
    Fut: Future<Output = error::Result<Resp>> + Send,
{
    loop {
        let client = manager.get_endpoint_client(endpoint).await?;

        let res = operation(client.clone()).await;
        return match res {
            Ok(r) => Ok(r),
            Err(e) => {
                if let Some(memdx_err) = e.is_memdx_error() {
                    if memdx_err.is_dispatch_error() {
                        // this was a dispatch error, so we can just try with
                        // a different client instead...
                        continue;
                    }
                }

                Err(e)
            }
        };
    }
}
