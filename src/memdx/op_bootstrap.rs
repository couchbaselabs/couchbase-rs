use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::request::HelloRequest;
use crate::memdx::response::{BootstrapResult, HelloResponse};
use std::sync::mpsc;
use std::sync::mpsc::Sender;

pub trait OpBootstrapEncoder {
    async fn hello<D>(
        &self,
        dispatcher: &mut D,
        request: HelloRequest,
        result_sender: Sender<Result<HelloResponse>>,
    ) -> Result<ClientPendingOp>
    where
        D: Dispatcher;
}

pub struct OpBootstrap {}

pub struct BootstrapOptions {
    pub hello: Option<HelloRequest>,
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
            encoder.hello(dispatcher, req, tx).await?;

            Some(rx)
        } else {
            None
        };
        let mut result = BootstrapResult { hello: None };

        if let Some(rx) = hello_rx {
            result.hello = Some(rx.recv().unwrap()?);
        }

        Ok(result)
    }
}
