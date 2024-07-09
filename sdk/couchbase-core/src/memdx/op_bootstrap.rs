use log::warn;
use tokio::select;
use tokio::time::{Instant, sleep};

use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::op_auth_saslauto::{OpSASLAutoEncoder, OpsSASLAuthAuto, SASLAuthAutoOptions};
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::{PendingOp, run_op_with_deadline, StandardPendingOp};
use crate::memdx::request::{
    GetClusterConfigRequest, GetErrorMapRequest, HelloRequest, SASLAuthRequest,
    SASLListMechsRequest, SASLStepRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    BootstrapResult, GetClusterConfigResponse, GetErrorMapResponse, HelloResponse,
    SASLAuthResponse, SASLListMechsResponse, SASLStepResponse, SelectBucketResponse,
};

pub trait OpBootstrapEncoder {
    fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<HelloResponse>>>
    where
        D: Dispatcher;

    fn get_error_map<D>(
        &self,
        dispatcher: &mut D,
        request: GetErrorMapRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<GetErrorMapResponse>>>
    where
        D: Dispatcher;

    fn select_bucket<D>(
        &self,
        dispatcher: &mut D,
        request: SelectBucketRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<SelectBucketResponse>>>
    where
        D: Dispatcher;

    fn get_cluster_config<D>(
        &self,
        dispatcher: &mut D,
        request: GetClusterConfigRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<GetClusterConfigResponse>>>
    where
        D: Dispatcher;
}

pub struct OpBootstrap {}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BootstrapOptions {
    pub hello: Option<HelloRequest>,
    pub get_error_map: Option<GetErrorMapRequest>,
    pub auth: Option<SASLAuthAutoOptions>,
    pub select_bucket: Option<SelectBucketRequest>,
    pub deadline: Instant,
    pub get_cluster_config: Option<GetClusterConfigRequest>,
}

impl OpBootstrap {
    // bootstrap is currently not pipelined. SCRAM, and the general retry behaviour within sasl auto,
    // make pipelining complex. It's a bit of a niche optimization so we can improve later.
    pub async fn bootstrap<E, D>(
        encoder: E,
        dispatcher: &mut D,
        opts: BootstrapOptions,
    ) -> Result<BootstrapResult>
    where
        E: OpBootstrapEncoder + OpSASLAutoEncoder,
        D: Dispatcher,
    {
        let mut result = BootstrapResult {
            hello: None,
            error_map: None,
            cluster_config: None,
        };

        if let Some(req) = opts.hello {
            let mut op = encoder.hello(dispatcher, req).await?;

            result.hello = match run_op_with_deadline(opts.deadline, &mut op).await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Hello failed {}", e);
                    None
                }
            };
        };

        if let Some(req) = opts.get_error_map {
            let mut op = encoder.get_error_map(dispatcher, req).await?;

            result.error_map = match run_op_with_deadline(opts.deadline, &mut op).await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get error map failed {}", e);
                    None
                }
            };
        };
        if let Some(req) = opts.auth {
            let op_auto = OpsSASLAuthAuto {};
            match op_auto
                .sasl_auth_auto(&encoder, dispatcher, opts.deadline, req)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Auth failed {}", e);
                }
            };
        }

        if let Some(req) = opts.select_bucket {
            let mut op = encoder.select_bucket(dispatcher, req).await?;

            match run_op_with_deadline(opts.deadline, &mut op).await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Select bucket failed {}", e);
                    None
                }
            };
        }

        if let Some(req) = opts.get_cluster_config {
            let mut op = encoder.get_cluster_config(dispatcher, req).await?;

            result.cluster_config = match run_op_with_deadline(opts.deadline, &mut op).await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get cluster config failed {}", e);
                    None
                }
            }
        };

        Ok(result)
    }
}
