use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::op_bootstrap::OpAuthEncoder;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::SASLAuthRequest;
use crate::memdx::response::SASLAuthResponse;

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

impl OpsCore {
    pub async fn sasl_auth_plain<D>(
        &self,
        dispatcher: &mut D,
        opts: SASLAuthPlainOptions,
        pipeline_cb: Option<impl (Fn()) + Send + Sync + 'static>,
    ) -> Result<StandardPendingOp<SASLAuthResponse>>
    where
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

        let op = self.sasl_auth(dispatcher, req).await?;

        if let Some(p_cb) = pipeline_cb {
            p_cb();
        }

        Ok(op)
    }
}
