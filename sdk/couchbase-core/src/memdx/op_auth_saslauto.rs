use std::cmp::PartialEq;

use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error;
use crate::memdx::error::CancellationErrorKind::{RequestCancelled, Timeout};
use crate::memdx::error::ErrorKind;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslbyname::{
    OpSASLAuthByNameEncoder, OpsSASLAuthByName, SASLAuthByNameOptions,
};
use crate::memdx::pendingop::{PendingOp, StandardPendingOp};
use crate::memdx::request::SASLListMechsRequest;
use crate::memdx::response::SASLListMechsResponse;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthAutoOptions {
    pub username: String,
    pub password: String,

    pub enabled_mechs: Vec<AuthMechanism>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsOptions {}

pub trait OpSASLAutoEncoder: OpSASLAuthByNameEncoder {
    fn sasl_list_mechs<D>(
        &self,
        dispatcher: &D,
        request: SASLListMechsRequest,
    ) -> impl std::future::Future<Output = Result<StandardPendingOp<SASLListMechsResponse>>>
    where
        D: Dispatcher;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLAuthAuto {}

impl OpsSASLAuthAuto {
    pub async fn sasl_auth_auto<E, D>(
        &self,
        encoder: &E,
        dispatcher: &D,
        deadline: Instant,
        opts: SASLAuthAutoOptions,
    ) -> Result<()>
    where
        E: OpSASLAutoEncoder,
        D: Dispatcher,
    {
        if opts.enabled_mechs.is_empty() {
            return Err(error::Error::invalid_argument_error(
                "no enabled mechanisms",
                "enabled_mechanisms",
            ));
        }

        let mut op = encoder
            .sasl_list_mechs(dispatcher, SASLListMechsRequest {})
            .await?;
        let server_mechs = op.recv().await?.available_mechs;

        // This unwrap is safe, we know it can't be None;
        let default_mech = opts.enabled_mechs.first().unwrap();

        let by_name = OpsSASLAuthByName {};
        match by_name
            .sasl_auth_by_name(
                encoder,
                dispatcher,
                SASLAuthByNameOptions {
                    username: opts.username.clone(),
                    password: opts.password.clone(),
                    auth_mechanism: default_mech.clone(),
                    deadline,
                },
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                match e.kind.as_ref() {
                    ErrorKind::Cancelled(Timeout) | ErrorKind::Cancelled(RequestCancelled) => {
                        return Err(e);
                    }
                    _ => {}
                }

                // There is no obvious way to differentiate between a mechanism being unsupported
                // and the credentials being wrong.  So for now we just assume any error should be
                // ignored if our list-mechs doesn't include the mechanism we used.
                // If the server supports the default mech, it means this error is 'real', otherwise
                // we try with one of the mechanisms that we now know are supported
                let supports_default_mech = server_mechs.contains(default_mech);
                if supports_default_mech {
                    return Err(e);
                }

                let selected_mech = opts
                    .enabled_mechs
                    .iter()
                    .find(|item| server_mechs.contains(item));

                let selected_mech = match selected_mech {
                    Some(mech) => mech,
                    None => {
                        return Err(ErrorKind::NoSupportedAuthMechanisms.into());
                    }
                };

                OpsSASLAuthByName {}
                    .sasl_auth_by_name(
                        encoder,
                        dispatcher,
                        SASLAuthByNameOptions {
                            username: opts.username.clone(),
                            password: opts.password.clone(),
                            auth_mechanism: selected_mech.clone(),
                            deadline,
                        },
                    )
                    .await
            }
        }
    }
}
