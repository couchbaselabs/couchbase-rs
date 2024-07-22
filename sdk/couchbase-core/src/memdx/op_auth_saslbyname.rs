use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslplain::{OpSASLPlainEncoder, OpsSASLAuthPlain, SASLAuthPlainOptions};
use crate::memdx::op_auth_saslscram::{OpSASLScramEncoder, OpsSASLAuthScram, SASLAuthScramOptions};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthByNameOptions {
    pub username: String,
    pub password: String,

    pub auth_mechanism: AuthMechanism,

    pub deadline: Instant,
}

pub trait OpSASLAuthByNameEncoder: OpSASLScramEncoder + OpSASLPlainEncoder {}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLAuthByName {}

impl OpsSASLAuthByName {
    pub async fn sasl_auth_by_name<E, D>(
        &self,
        encoder: &E,
        dispatcher: &D,
        opts: SASLAuthByNameOptions,
    ) -> Result<()>
    where
        E: OpSASLAuthByNameEncoder,
        D: Dispatcher,
    {
        match opts.auth_mechanism {
            AuthMechanism::Plain => {
                OpsSASLAuthPlain {}
                    .sasl_auth_plain(
                        encoder,
                        dispatcher,
                        SASLAuthPlainOptions::new(opts.username, opts.password, opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha1 => {
                OpsSASLAuthScram {}
                    .sasl_auth_scram_1(
                        encoder,
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password, opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha256 => {
                OpsSASLAuthScram {}
                    .sasl_auth_scram_256(
                        encoder,
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password, opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha512 => {
                OpsSASLAuthScram {}
                    .sasl_auth_scram_512(
                        encoder,
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password, opts.deadline),
                    )
                    .await
            }
        }
    }
}
