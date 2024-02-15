use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::request::{
    GetErrorMapRequest, HelloRequest, SASLAuthRequest, SelectBucketRequest,
};
use crate::memdx::response::{
    BootstrapResult, GetErrorMapResponse, HelloResponse, SASLAuthResponse, SelectBucketResponse,
};
use log::warn;
use std::sync::mpsc;

pub trait OpBootstrapEncoder {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
        cb: impl (Fn(Result<HelloResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher;

    async fn get_error_map<D>(
        &self,
        dispatcher: &mut D,
        request: GetErrorMapRequest,
        cb: impl (Fn(Result<GetErrorMapResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher;

    async fn select_bucket<D>(
        &self,
        dispatcher: &mut D,
        request: SelectBucketRequest,
        cb: impl (Fn(Result<SelectBucketResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher;

    async fn sasl_auth<D>(
        &self,
        dispatcher: &mut D,
        request: SASLAuthRequest,
        cb: impl (Fn(Result<SASLAuthResponse>)) + Send + Sync + 'static,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher;
}

pub struct OpBootstrap {}

pub struct BootstrapOptions {
    pub hello: Option<HelloRequest>,
    pub get_error_map: Option<GetErrorMapRequest>,
    pub auth: Option<SASLAuthRequest>,
    pub select_bucket: Option<SelectBucketRequest>,
}

impl OpBootstrap {
    pub async fn bootstrap<E, D>(
        encoder: E,
        dispatcher: &mut D,
        opts: BootstrapOptions,
    ) -> Result<BootstrapResult>
    where
        E: OpBootstrapEncoder,
        D: Dispatcher,
    {
        let hello_rx = if let Some(req) = opts.hello {
            let (tx, rx) = mpsc::channel::<Result<HelloResponse>>();
            encoder
                .hello(dispatcher, req, move |response| {
                    tx.send(response).unwrap();
                })
                .await?;

            Some(rx)
        } else {
            None
        };
        let error_map_rx = if let Some(req) = opts.get_error_map {
            let (tx, rx) = mpsc::channel::<Result<GetErrorMapResponse>>();
            encoder
                .get_error_map(dispatcher, req, move |response| {
                    tx.send(response).unwrap();
                })
                .await?;

            Some(rx)
        } else {
            None
        };
        let auth_rx = if let Some(req) = opts.auth {
            let (tx, rx) = mpsc::channel::<Result<SASLAuthResponse>>();
            encoder
                .sasl_auth(dispatcher, req, move |response| {
                    tx.send(response).unwrap();
                })
                .await?;

            Some(rx)
        } else {
            None
        };
        let select_bucket_rx = if let Some(req) = opts.select_bucket {
            let (tx, rx) = mpsc::channel::<Result<SelectBucketResponse>>();
            encoder
                .select_bucket(dispatcher, req, move |response| {
                    tx.send(response).unwrap();
                })
                .await?;

            Some(rx)
        } else {
            None
        };
        let mut result = BootstrapResult {
            hello: None,
            error_map: None,
        };

        if let Some(rx) = hello_rx {
            result.hello = match rx.recv().unwrap() {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Hello failed {}", e);
                    None
                }
            };
        }
        if let Some(rx) = error_map_rx {
            result.error_map = match rx.recv().unwrap() {
                Ok(r) => Some(r),
                Err(e) => {
                    warn!("Get error map failed {}", e);
                    None
                }
            };
        }
        if let Some(rx) = auth_rx {
            match rx.recv().unwrap() {
                Ok(r) => {}
                Err(e) => {
                    warn!("Auth failed {}", e);
                    return Err(e);
                }
            }
        }
        if let Some(rx) = select_bucket_rx {
            match rx.recv().unwrap() {
                Ok(r) => {}
                Err(e) => {
                    warn!("Select bucket failed {}", e);
                    return Err(e);
                }
            }
        }

        Ok(result)
    }
}
