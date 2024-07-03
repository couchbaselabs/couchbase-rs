use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::pendingop::{run_op_with_deadline, StandardPendingOp};
use crate::memdx::request::SASLAuthRequest;
use crate::memdx::response::SASLAuthResponse;

pub trait OpSASLPlainEncoder {
    async fn sasl_auth<D>(
        &self,
        dispatcher: &mut D,
        req: SASLAuthRequest,
    ) -> Result<StandardPendingOp<SASLAuthResponse>>
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
        dispatcher: &mut D,
        opts: SASLAuthPlainOptions,
    ) -> Result<()>
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

        let mut op = encoder.sasl_auth(dispatcher, req).await?;

        let resp = run_op_with_deadline(opts.deadline, &mut op).await?;

        if resp.needs_more_steps {
            return Err(Error::Protocol(
                "Server did not accept auth when the client expected".to_string(),
            ));
        }

        Ok(())
    }
}
