use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::pendingop::StandardPendingOp;
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
    username: String,
    password: String,
}

impl SASLAuthPlainOptions {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
    pub fn username(&self) -> String {
        self.username.clone()
    }
    pub fn password(&self) -> String {
        self.password.clone()
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
    ) -> Result<StandardPendingOp<SASLAuthResponse>>
    where
        E: OpSASLPlainEncoder,
        D: Dispatcher,
    {
        let mut payload: Vec<u8> = Vec::new();
        payload.push(0);
        payload.extend_from_slice(opts.username().as_ref());
        payload.push(0);
        payload.extend_from_slice(opts.password().as_ref());

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::Plain,
        };

        let op = encoder.sasl_auth(dispatcher, req).await?;

        Ok(op)
    }
}
