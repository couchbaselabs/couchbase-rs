use std::future::Future;

use crate::error::Result;
use crate::error::{Error, MemdxError};
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
    GetRequest, IncrementRequest, LookupInRequest, MutateInRequest, PrependRequest, ReplaceRequest,
    SelectBucketRequest, SetRequest, TouchRequest, UnlockRequest,
};
use crate::memdx::response::{
    AddResponse, AppendResponse, BootstrapResult, DecrementResponse, DeleteResponse,
    GetAndLockResponse, GetAndTouchResponse, GetClusterConfigResponse, GetCollectionIdResponse,
    GetMetaResponse, GetResponse, IncrementResponse, LookupInResponse, MutateInResponse,
    PrependResponse, ReplaceResponse, SelectBucketResponse, SetResponse, TouchResponse,
    UnlockResponse,
};

pub(crate) trait KvClientOps: Sized + Send + Sync {
    fn set(&self, req: SetRequest) -> impl Future<Output = Result<SetResponse>> + Send;
    fn get(&self, req: GetRequest) -> impl Future<Output = Result<GetResponse>> + Send;
    fn get_meta(&self, req: GetMetaRequest)
        -> impl Future<Output = Result<GetMetaResponse>> + Send;
    fn delete(&self, req: DeleteRequest) -> impl Future<Output = Result<DeleteResponse>> + Send;
    fn get_and_lock(
        &self,
        req: GetAndLockRequest,
    ) -> impl Future<Output = Result<GetAndLockResponse>> + Send;
    fn get_and_touch(
        &self,
        req: GetAndTouchRequest,
    ) -> impl Future<Output = Result<GetAndTouchResponse>> + Send;
    fn unlock(&self, req: UnlockRequest) -> impl Future<Output = Result<UnlockResponse>> + Send;
    fn touch(&self, req: TouchRequest) -> impl Future<Output = Result<TouchResponse>> + Send;
    fn add(&self, req: AddRequest) -> impl Future<Output = Result<AddResponse>> + Send;
    fn replace(&self, req: ReplaceRequest) -> impl Future<Output = Result<ReplaceResponse>> + Send;
    fn append(&self, req: AppendRequest) -> impl Future<Output = Result<AppendResponse>> + Send;
    fn prepend(&self, req: PrependRequest) -> impl Future<Output = Result<PrependResponse>> + Send;
    fn increment(
        &self,
        req: IncrementRequest,
    ) -> impl Future<Output = Result<IncrementResponse>> + Send;
    fn decrement(
        &self,
        req: DecrementRequest,
    ) -> impl Future<Output = Result<DecrementResponse>> + Send;

    fn lookup_in(
        &self,
        req: LookupInRequest,
    ) -> impl Future<Output = Result<LookupInResponse>> + Send;

    fn mutate_in(
        &self,
        req: MutateInRequest,
    ) -> impl Future<Output = Result<MutateInResponse>> + Send;
    fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> impl Future<Output = Result<GetClusterConfigResponse>> + Send;
    fn get_collection_id(
        &self,
        req: GetCollectionIdRequest,
    ) -> impl Future<Output = Result<GetCollectionIdResponse>> + Send;
}

impl<D> KvClientOps for StdKvClient<D>
where
    D: Dispatcher,
{
    async fn set(&self, req: SetRequest<'_>) -> Result<SetResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().set(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get(&self, req: GetRequest<'_>) -> Result<GetResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().get(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_meta(&self, req: GetMetaRequest<'_>) -> Result<GetMetaResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().get_meta(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn delete(&self, req: DeleteRequest<'_>) -> Result<DeleteResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().delete(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_and_lock(&self, req: GetAndLockRequest<'_>) -> Result<GetAndLockResponse> {
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get_and_lock(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_and_touch(&self, req: GetAndTouchRequest<'_>) -> Result<GetAndTouchResponse> {
        let mut op = self
            .handle_dispatch_side_result(self.ops_crud().get_and_touch(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn unlock(&self, req: UnlockRequest<'_>) -> Result<UnlockResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().unlock(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn touch(&self, req: TouchRequest<'_>) -> Result<TouchResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().touch(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn add(&self, req: AddRequest<'_>) -> Result<AddResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().add(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn replace(&self, req: ReplaceRequest<'_>) -> Result<ReplaceResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().replace(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn append(&self, req: AppendRequest<'_>) -> Result<AppendResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().append(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn prepend(&self, req: PrependRequest<'_>) -> Result<PrependResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().prepend(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn increment(&self, req: IncrementRequest<'_>) -> Result<IncrementResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().increment(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn decrement(&self, req: DecrementRequest<'_>) -> Result<DecrementResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().decrement(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn lookup_in(&self, req: LookupInRequest<'_>) -> Result<LookupInResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().lookup_in(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn mutate_in(&self, req: MutateInRequest<'_>) -> Result<MutateInResponse> {
        let mut op =
            self.handle_dispatch_side_result(self.ops_crud().mutate_in(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> Result<GetClusterConfigResponse> {
        let mut op = self
            .handle_dispatch_side_result(OpsCore {}.get_cluster_config(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }

    async fn get_collection_id(
        &self,
        req: GetCollectionIdRequest<'_>,
    ) -> Result<GetCollectionIdResponse> {
        let mut op = self
            .handle_dispatch_side_result(OpsUtil {}.get_collection_id(self.client(), req).await)?;

        let res = self.handle_response_side_result(op.recv().await)?;
        Ok(res)
    }
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    fn handle_dispatch_side_result<T>(&self, result: memdx::error::Result<T>) -> Result<T> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                let e = if e.is_dispatch_error() {
                    let mut e = MemdxError::new(e)
                        .with_dispatched_to(self.remote_addr().to_string())
                        .with_dispatched_from(self.local_addr().to_string());
                    Error::new_contextual_memdx_error(e)
                } else {
                    Error::new_contextual_memdx_error(MemdxError::new(e))
                };

                Err(e)
            }
        }
    }

    fn handle_response_side_result<T>(&self, result: memdx::error::Result<T>) -> Result<T> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new_contextual_memdx_error(
                MemdxError::new(e)
                    .with_dispatched_to(self.remote_addr().to_string())
                    .with_dispatched_from(self.local_addr().to_string()),
            )),
        }
    }

    pub async fn bootstrap(&self, opts: BootstrapOptions) -> Result<BootstrapResult> {
        OpBootstrap::bootstrap(OpsCore {}, self.client(), opts)
            .await
            .map_err(|e| {
                Error::new_contextual_memdx_error(
                    MemdxError::new(e)
                        .with_dispatched_to(self.remote_addr().to_string())
                        .with_dispatched_from(self.local_addr().to_string()),
                )
            })
    }

    pub async fn select_bucket(&self, req: SelectBucketRequest) -> Result<SelectBucketResponse> {
        let mut op =
            self.handle_dispatch_side_result(OpsCore {}.select_bucket(self.client(), req).await)?;

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
