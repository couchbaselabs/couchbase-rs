use std::future::Future;

use crate::error::Error;
use crate::error::Result;
use crate::kvclient::{KvClient, StdKvClient};
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap, OpBootstrapEncoder};
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::OpsCrud;
use crate::memdx::pendingop::PendingOp;
use crate::memdx::request::{GetClusterConfigRequest, GetRequest, SelectBucketRequest, SetRequest};
use crate::memdx::response::{
    BootstrapResult, GetClusterConfigResponse, GetResponse, SelectBucketResponse, SetResponse,
};

pub(crate) trait KvClientOps: Sized + Send + Sync {
    fn set(&self, req: SetRequest) -> impl Future<Output = Result<SetResponse>> + Send;
    fn get(&self, req: GetRequest) -> impl Future<Output = Result<GetResponse>> + Send;
    fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> impl Future<Output = Result<GetClusterConfigResponse>> + Send;
}

impl<D> KvClientOps for StdKvClient<D>
where
    D: Dispatcher,
{
    async fn set(&self, req: SetRequest) -> Result<SetResponse> {
        let mut op = self.ops_crud().set(self.client(), req).await?;

        let res = op.recv().await?;
        Ok(res)
    }

    async fn get(&self, req: GetRequest) -> Result<GetResponse> {
        let mut op = self.ops_crud().get(self.client(), req).await?;

        let res = op.recv().await?;
        Ok(res)
    }

    async fn get_cluster_config(
        &self,
        req: GetClusterConfigRequest,
    ) -> Result<GetClusterConfigResponse> {
        let mut op = OpsCore {}.get_cluster_config(self.client(), req).await?;

        let res = op.recv().await?;
        Ok(res)
    }
}

impl<D> StdKvClient<D>
where
    D: Dispatcher,
{
    pub async fn bootstrap(&self, opts: BootstrapOptions) -> Result<BootstrapResult> {
        OpBootstrap::bootstrap(OpsCore {}, self.client(), opts)
            .await
            .map_err(Error::from)
    }

    pub async fn select_bucket(&self, req: SelectBucketRequest) -> Result<SelectBucketResponse> {
        let mut op = OpsCore {}
            .select_bucket(self.client(), req)
            .await
            .map_err(Error::from)?;

        let res = op.recv().await?;
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
