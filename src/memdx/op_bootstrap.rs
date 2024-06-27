use log::warn;
use tokio::select;
use tokio::time::Instant;

use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::CancellationErrorKind;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{
    GetErrorMapRequest, HelloRequest, SASLAuthRequest, SASLListMechsRequest, SASLStepRequest,
    SelectBucketRequest,
};
use crate::memdx::response::{
    BootstrapResult, GetErrorMapResponse, HelloResponse, SASLAuthResponse, SASLListMechsResponse,
    SASLStepResponse, SelectBucketResponse, TryFromClientResponse,
};

pub trait OpAuthEncoder {
    async fn sasl_auth<D>(
        &self,
        dispatcher: &mut D,
        request: SASLAuthRequest,
    ) -> Result<StandardPendingOp<SASLAuthResponse>>
    where
        D: Dispatcher;

    async fn sasl_step<D>(
        &self,
        dispatcher: &mut D,
        request: SASLStepRequest,
    ) -> Result<StandardPendingOp<SASLStepResponse>>
    where
        D: Dispatcher;
}

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

    async fn sasl_list_mechs<D>(
        &self,
        dispatcher: &mut D,
        request: SASLListMechsRequest,
    ) -> Result<StandardPendingOp<SASLListMechsResponse>>
    where
        D: Dispatcher;
}

pub struct OpBootstrap {}

pub struct BootstrapOptions {
    pub hello: Option<HelloRequest>,
    pub get_error_map: Option<GetErrorMapRequest>,
    pub auth: Option<SASLAuthRequest>,
    pub select_bucket: Option<SelectBucketRequest>,
    pub deadline: Instant,
}

impl OpBootstrap {
    pub async fn bootstrap<E, D>(
        encoder: E,
        dispatcher: &mut D,
        opts: BootstrapOptions,
    ) -> Result<BootstrapResult>
    where
        E: OpBootstrapEncoder + OpAuthEncoder,
        D: Dispatcher,
    {
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
        let auth_op = if let Some(req) = opts.auth {
            Some(encoder.sasl_auth(dispatcher, req).await?)
        } else {
            None
        };
        let select_bucket_op = if let Some(req) = opts.select_bucket {
            Some(encoder.select_bucket(dispatcher, req).await?)
        } else {
            None
        };
        let mut result = BootstrapResult {
            hello: None,
            error_map: None,
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
        if let Some(mut op) = auth_op {
            match op.recv().await {
                Ok(_r) => {}
                Err(e) => {
                    warn!("Auth failed {}", e);
                    return Err(e);
                }
            }
        }
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

async fn await_bootstrap_op<T: TryFromClientResponse>(
    deadline: Instant,
    mut op: StandardPendingOp<T>,
) -> Result<T> {
    select! {
        res = op.recv() => res,
        _ = tokio::time::sleep_until(deadline) => {
            op.cancel(CancellationErrorKind::Timeout);
            op.recv().await
        }
    }
}
