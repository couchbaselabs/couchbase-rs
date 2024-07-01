use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::op_auth_saslplain::{OpSASLPlainEncoder, SASLAuthPlainOptions};
use crate::memdx::op_auth_saslscram::{OpSASLScramEncoder, SASLAuthScramOptions};
use crate::memdx::ops_core::OpsCore;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthByNameOptions {
    pub username: String,
    pub password: String,

    pub auth_mechanism: AuthMechanism,
}

pub trait OpSASLAuthByNameEncoder: OpSASLScramEncoder + OpSASLPlainEncoder {
    async fn sasl_auth_by_name<D>(
        &self,
        dispatcher: &mut D,
        opts: SASLAuthByNameOptions,
    ) -> Result<()>
    where
        D: Dispatcher;
}

impl OpSASLAuthByNameEncoder for OpsCore {
    async fn sasl_auth_by_name<D>(
        &self,
        dispatcher: &mut D,
        opts: SASLAuthByNameOptions,
    ) -> Result<()>
    where
        D: Dispatcher,
    {
        match opts.auth_mechanism {
            AuthMechanism::Plain => {
                let mut op = self
                    .sasl_auth_plain(
                        dispatcher,
                        SASLAuthPlainOptions::new(opts.username, opts.password),
                    )
                    .await?;
                op.recv().await?;
                return Ok(());
            }
            AuthMechanism::ScramSha1 => {
                return self
                    .sasl_auth_scram_1(
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password),
                    )
                    .await
            }
            AuthMechanism::ScramSha256 => {
                return self
                    .sasl_auth_scram_256(
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password),
                    )
                    .await
            }
            AuthMechanism::ScramSha512 => {
                return self
                    .sasl_auth_scram_512(
                        dispatcher,
                        SASLAuthScramOptions::new(opts.username, opts.password),
                    )
                    .await
            }
        }
    }
}
