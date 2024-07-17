use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::MemdxResult;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::MemdxError;
use crate::memdx::pendingop::{run_op_future_with_deadline, StandardPendingOp};
use crate::memdx::request::SASLAuthRequest;
use crate::memdx::response::SASLAuthResponse;

pub trait OpSASLPlainEncoder {
    fn sasl_auth<D>(
        &self,
        dispatcher: &D,
        req: SASLAuthRequest,
    ) -> impl std::future::Future<Output = MemdxResult<StandardPendingOp<SASLAuthResponse>>>
    where
        D: Dispatcher;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthPlainOptions {
    pub username: String,
    pub password: String,
    pub deadline: Instant,
}

impl SASLAuthPlainOptions {
    pub fn new(username: String, password: String, deadline: Instant) -> Self {
        Self {
            username,
            password,
            deadline,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLAuthPlain {}

impl OpsSASLAuthPlain {
    pub async fn sasl_auth_plain<E, D>(
        &self,
        encoder: &E,
        dispatcher: &D,
        opts: SASLAuthPlainOptions,
    ) -> MemdxResult<()>
    where
        E: OpSASLPlainEncoder,
        D: Dispatcher,
    {
        let mut payload: Vec<u8> = Vec::new();
        payload.push(0);
        payload.extend_from_slice(opts.username.as_ref());
        payload.push(0);
        payload.extend_from_slice(opts.password.as_ref());

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::Plain,
        };

        let resp =
            run_op_future_with_deadline(opts.deadline, encoder.sasl_auth(dispatcher, req)).await?;

        if resp.needs_more_steps {
            return Err(MemdxError::Protocol(
                "Server did not accept auth when the client expected".to_string(),
            ));
        }

        Ok(())
    }
}
