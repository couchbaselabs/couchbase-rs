use hmac::digest::{Digest, KeyInit};
use hmac::Mac;
use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::{run_op_future_with_deadline, StandardPendingOp};
use crate::memdx::request::{SASLAuthRequest, SASLStepRequest};
use crate::memdx::response::SASLStepResponse;
use crate::scram::Client;

pub trait OpSASLScramEncoder: OpSASLPlainEncoder {
    fn sasl_step<D>(
        &self,
        dispatcher: &D,
        request: SASLStepRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<SASLStepResponse>>>
    where
        D: Dispatcher;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthScramOptions {
    deadline: Instant,
}

impl SASLAuthScramOptions {
    pub fn new(deadline: Instant) -> Self {
        Self { deadline }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLAuthScram {}

impl OpsSASLAuthScram {
    pub async fn sasl_auth_scram<E, D, Di, H>(
        &self,
        encoder: &E,
        dispatcher: &D,
        mut client: Client<Di, H>,
        opts: SASLAuthScramOptions,
    ) -> Result<()>
    where
        E: OpSASLScramEncoder,
        D: Dispatcher,
        Di: Mac + KeyInit,
        H: Digest,
    {
        // Perform the initial SASL step
        let payload = client.step1().map_err(|e| {
            Error::protocol_error_with_source("failed to perform step1", Box::new(e))
        })?;

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha512,
        };

        let resp =
            run_op_future_with_deadline(opts.deadline, encoder.sasl_auth(dispatcher, req)).await?;

        if !resp.needs_more_steps {
            return Ok(());
        }

        let payload = client.step2(resp.payload.as_slice()).map_err(|e| {
            Error::protocol_error_with_source("failed to perform step2", Box::new(e))
        })?;

        let req = SASLStepRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha512,
        };

        let resp =
            run_op_future_with_deadline(opts.deadline, encoder.sasl_step(dispatcher, req)).await?;

        if resp.needs_more_steps {
            return Err(Error::protocol_error(
                "Server did not accept auth when the client expected",
            ));
        }

        Ok(())
    }
}
