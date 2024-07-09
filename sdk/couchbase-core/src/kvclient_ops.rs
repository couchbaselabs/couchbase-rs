use crate::error::CoreError;
use crate::kvclient::StdKvClient;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::op_bootstrap::{BootstrapOptions, OpBootstrap};
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::OpsCrud;
use crate::memdx::pendingop::PendingOp;
use crate::memdx::request::{GetRequest, SetRequest};
use crate::memdx::response::{BootstrapResult, GetResponse, SetResponse};
use crate::result::CoreResult;

impl StdKvClient {
    pub async fn bootstrap(&mut self, opts: BootstrapOptions) -> CoreResult<BootstrapResult> {
        OpBootstrap::bootstrap(OpsCore {}, self.client_mut(), opts)
            .await
            .map_err(CoreError::from)
    }

    pub async fn get(&mut self, req: GetRequest) -> CoreResult<GetResponse> {
        let mut op = self.ops_crud().get(self.client_mut(), req).await?;

        let res = op.recv().await?;
        Ok(res)
    }

    pub async fn set(&mut self, req: SetRequest) -> CoreResult<SetResponse> {
        let mut op = self.ops_crud().set(self.client_mut(), req).await?;

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
