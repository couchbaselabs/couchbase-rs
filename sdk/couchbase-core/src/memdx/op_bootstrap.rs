use log::warn;
use tokio::select;
use tokio::time::{sleep, Instant};

use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslauto::{OpSASLAutoEncoder, OpsSASLAuthAuto, SASLAuthAutoOptions};
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::{run_op_future_with_deadline, PendingOp, StandardPendingOp};
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
        dispatcher: &D,
        request: HelloRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<HelloResponse>>>
    where
        D: Dispatcher;

    fn get_error_map<D>(
        &self,
        dispatcher: &D,
        request: GetErrorMapRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<GetErrorMapResponse>>>
    where
        D: Dispatcher;

    fn select_bucket<D>(
        &self,
        dispatcher: &D,
        request: SelectBucketRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<SelectBucketResponse>>>
    where
        D: Dispatcher;

    fn get_cluster_config<D>(
        &self,
        dispatcher: &D,
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
        dispatcher: &D,
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
            result.hello =
                match run_op_future_with_deadline(opts.deadline, encoder.hello(dispatcher, req))
                    .await
                {
                    Ok(r) => Some(r),
                    Err(e) => {
                        warn!(context=dispatcher.log_context(), e:err; "Hello failed");
                        None
                    }
                };
        };

        if let Some(req) = opts.get_error_map {
            result.error_map = match run_op_future_with_deadline(
                opts.deadline,
                encoder.get_error_map(dispatcher, req),
            )
            .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(context=dispatcher.log_context(), e:err; "Get error map failed");
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
                    warn!(context=dispatcher.log_context(), e:err; "Auth failed");
                    return Err(e);
                }
            };
        }

        if let Some(req) = opts.select_bucket {
            match run_op_future_with_deadline(opts.deadline, encoder.select_bucket(dispatcher, req))
                .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(context=dispatcher.log_context(), e:err; "Select bucket failed");
                    return Err(e);
                }
            };
        }

        if let Some(req) = opts.get_cluster_config {
            result.cluster_config = match run_op_future_with_deadline(
                opts.deadline,
                encoder.get_cluster_config(dispatcher, req),
            )
            .await
            {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!(
                        context=dispatcher.log_context(), e:err;
                        "Get cluster config failed",
                    );
                    None
                }
            }
        };

        Ok(result)
    }
}
