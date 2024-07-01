use log::warn;
use tokio::time::Instant;

use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::op_auth_saslauto::{OpSASLAutoEncoder, OpsSASLAuthAuto, SASLAuthAutoOptions};
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    GetErrorMapRequest, HelloRequest, SASLAuthRequest, SASLListMechsRequest, SASLStepRequest,
    SelectBucketRequest,
};
use crate::memdx::response::{
    BootstrapResult, GetErrorMapResponse, HelloResponse, SASLAuthResponse, SASLListMechsResponse,
    SASLStepResponse, SelectBucketResponse,
};

pub trait OpBootstrapEncoder {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
    ) -> Result<StandardPendingOp<HelloResponse>>
    where
        D: Dispatcher;

    async fn get_error_map<D>(
        &self,
        dispatcher: &mut D,
        request: GetErrorMapRequest,
    ) -> Result<StandardPendingOp<GetErrorMapResponse>>
    where
        D: Dispatcher;

    async fn select_bucket<D>(
        &self,
        dispatcher: &mut D,
        request: SelectBucketRequest,
    ) -> Result<StandardPendingOp<SelectBucketResponse>>
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
        };

        let hello_op = if let Some(req) = opts.hello {
            Some(encoder.hello(dispatcher, req).await?)
        } else {
            None
        };
        let error_map_op = if let Some(req) = opts.get_error_map {
            Some(encoder.get_error_map(dispatcher, req).await?)
        } else {
            None
        };

        if let Some(mut op) = hello_op {
            result.hello = match op.recv().await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Hello failed {}", e);
                    None
                }
            };
        }
        if let Some(mut op) = error_map_op {
            result.error_map = match op.recv().await {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get error map failed {}", e);
                    None
                }
            };
        }

        if let Some(req) = opts.auth {
            OpsSASLAuthAuto {}
                .sasl_auth_auto(&encoder, dispatcher, req)
                .await?;
        }

        let select_bucket_op = if let Some(req) = opts.select_bucket {
            Some(encoder.select_bucket(dispatcher, req).await?)
        } else {
            None
        };

        if let Some(mut op) = select_bucket_op {
            match op.recv().await {
                Ok(_r) => {}
                Err(e) => {
                    warn!("Select bucket failed {}", e);
                    return Err(e);
                }
            }
        }

        Ok(result)
    }
}
