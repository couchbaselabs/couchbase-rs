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

use crate::error::MemdxError;
use crate::kvclient::{KvClient, StdKvClient};
use crate::memdx;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap, OpBootstrapEncoder};
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::OpsCrud;
use crate::memdx::ops_util::OpsUtil;
use crate::memdx::pendingop::PendingOp;
use crate::memdx::request::{
    AddRequest, AppendRequest, DecrementRequest, DeleteRequest, GetAndLockRequest,
    GetAndTouchRequest, GetClusterConfigRequest, GetCollectionIdRequest, GetMetaRequest,
    GetRequest, IncrementRequest, LookupInRequest, MutateInRequest, PingRequest, PrependRequest,
    ReplaceRequest, SelectBucketRequest, SetRequest, TouchRequest, UnlockRequest,
};
use crate::memdx::response::{
    AddResponse, AppendResponse, BootstrapResult, DecrementResponse, DeleteResponse,
    GetAndLockResponse, GetAndTouchResponse, GetClusterConfigResponse, GetCollectionIdResponse,
    GetMetaResponse, GetResponse, IncrementResponse, LookupInResponse, MutateInResponse,
    PingResponse, PrependResponse, ReplaceResponse, SelectBucketResponse, SetResponse,
    TouchResponse, UnlockResponse,
};
use chrono::Utc;
use std::future::Future;
use std::sync::atomic::Ordering;

pub(crate) type KvResult<T> = Result<T, MemdxError>;

pub(crate) trait KvClientOps: Sized + Send + Sync {
    fn bucket_name(&self) -> Option<String>;
    fn set(&self, req: SetRequest) -> impl Future<Output = KvResult<SetResponse>> + Send;
    fn get(&self, req: GetRequest) -> impl Future<Output = KvResult<GetResponse>> + Send;
    fn get_meta(
        &self,
        req: GetMetaRequest,
    ) -> impl Future<Output = KvResult<GetMetaResponse>> + Send;
    fn delete(&self, req: DeleteRequest) -> impl Future<Output = KvResult<DeleteResponse>> + Send;
    fn get_and_lock(
        &self,
        req: GetAndLockRequest,
    ) -> impl Future<Output = KvResult<GetAndLockResponse>> + Send;
    fn get_and_touch(
        &self,
        req: GetAndTouchRequest,
    ) -> impl Future<Output = KvResult<GetAndTouchResponse>> + Send;
    fn unlock(&self, req: UnlockRequest) -> impl Future<Output = KvResult<UnlockResponse>> + Send;
    fn touch(&self, req: TouchRequest) -> impl Future<Output = KvResult<TouchResponse>> + Send;
    fn add(&self, req: AddRequest) -> impl Future<Output = KvResult<AddResponse>> + Send;
    fn replace(
        &self,
        req: ReplaceRequest,
    ) -> impl Future<Output = KvResult<ReplaceResponse>> + Send;
    fn append(&self, req: AppendRequest) -> impl Future<Output = KvResult<AppendResponse>> + Send;
    fn prepend(
        &self,
        req: PrependRequest,
    ) -> impl Future<Output = KvResult<PrependResponse>> + Send;
    fn increment(
        &self,
        req: IncrementRequest,
    ) -> impl Future<Output = KvResult<IncrementResponse>> + Send;
    fn decrement(
        &self,
        req: DecrementRequest,
    ) -> impl Future<Output = KvResult<DecrementResponse>> + Send;

    fn lookup_in(
        &self,
        req: LookupInRequest,
    ) -> impl Future<Output = KvResult<LookupInResponse>> + Send;

    fn mutate_in(
        &self,
        req: MutateInRequest,
    ) -> impl Future<Output = KvResult<MutateInResponse>> + Send;
    fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> impl Future<Output = KvResult<GetClusterConfigResponse>> + Send;
    fn get_collection_id(
        &self,
        req: GetCollectionIdRequest,
    ) -> impl Future<Output = KvResult<GetCollectionIdResponse>> + Send;
    fn ping(&self, req: PingRequest) -> impl Future<Output = KvResult<PingResponse>> + Send;
}

impl<D> KvClientOps for StdKvClient<D>
where
    D: Dispatcher,
{
    fn bucket_name(&self) -> Option<String> {
        self.selected_bucket.lock().unwrap().clone()
    }

    async fn set(&self, req: SetRequest<'_>) -> KvResult<SetResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().set(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get(&self, req: GetRequest<'_>) -> KvResult<GetResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_meta(&self, req: GetMetaRequest<'_>) -> KvResult<GetMetaResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get_meta(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn delete(&self, req: DeleteRequest<'_>) -> KvResult<DeleteResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().delete(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_and_lock(&self, req: GetAndLockRequest<'_>) -> KvResult<GetAndLockResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get_and_lock(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_and_touch(&self, req: GetAndTouchRequest<'_>) -> KvResult<GetAndTouchResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get_and_touch(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn unlock(&self, req: UnlockRequest<'_>) -> KvResult<UnlockResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().unlock(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn touch(&self, req: TouchRequest<'_>) -> KvResult<TouchResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().touch(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn add(&self, req: AddRequest<'_>) -> KvResult<AddResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().add(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn replace(&self, req: ReplaceRequest<'_>) -> KvResult<ReplaceResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().replace(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn append(&self, req: AppendRequest<'_>) -> KvResult<AppendResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().append(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn prepend(&self, req: PrependRequest<'_>) -> KvResult<PrependResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().prepend(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn increment(&self, req: IncrementRequest<'_>) -> KvResult<IncrementResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().increment(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn decrement(&self, req: DecrementRequest<'_>) -> KvResult<DecrementResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().decrement(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn lookup_in(&self, req: LookupInRequest<'_>) -> KvResult<LookupInResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().lookup_in(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn mutate_in(&self, req: MutateInRequest<'_>) -> KvResult<MutateInResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().mutate_in(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> KvResult<GetClusterConfigResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(OpsCore {}.get_cluster_config(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_collection_id(
        &self,
        req: GetCollectionIdRequest<'_>,
    ) -> KvResult<GetCollectionIdResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(OpsUtil {}.get_collection_id(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn ping(&self, req: PingRequest<'_>) -> KvResult<PingResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(OpsUtil {}.ping(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    fn update_last_activity(&self) {
        self.last_activity_timestamp_micros
            .store(Utc::now().timestamp_micros(), Ordering::SeqCst);
    }

    async fn handle_dispatch_side_result<T>(&self, result: memdx::error::Result<T>) -> KvResult<T> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                if let memdx::error::ErrorKind::Dispatch { .. } = e.kind() {
                    self.mark_closed().await;
                }

                Err(MemdxError::new(e)
                    .with_dispatched_to(self.remote_addr().to_string())
                    .with_dispatched_from(self.local_addr().to_string()))
            }
        }
    }

    fn handle_response_side_result<T>(&self, result: memdx::error::Result<T>) -> KvResult<T> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(MemdxError::new(e)
                .with_dispatched_to(self.remote_addr().to_string())
                .with_dispatched_from(self.local_addr().to_string())),
        }
    }

    pub async fn bootstrap(&self, opts: BootstrapOptions) -> KvResult<BootstrapResult> {
        OpBootstrap::bootstrap(OpsCore {}, self.client(), opts)
            .await
            .map_err(|e| {
                MemdxError::new(e)
                    .with_dispatched_to(self.remote_addr().to_string())
                    .with_dispatched_from(self.local_addr().to_string())
            })
    }

    pub async fn select_bucket(&self, req: SelectBucketRequest) -> KvResult<SelectBucketResponse> {
        self.update_last_activity();
        let mut op = self
            .handle_dispatch_side_result(OpsCore {}.select_bucket(self.client(), req).await)
            .await?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    fn ops_crud(&self) -> OpsCrud {
        OpsCrud {
            collections_enabled: self.has_feature(HelloFeature::Collections),
            durability_enabled: self.has_feature(HelloFeature::SyncReplication),
            preserve_expiry_enabled: self.has_feature(HelloFeature::PreserveExpiry),
            ext_frames_enabled: self.has_feature(HelloFeature::AltRequests),
        }
    }
}
