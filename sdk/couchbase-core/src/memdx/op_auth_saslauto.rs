/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::cmp::PartialEq;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslbyname::{
    OpSASLAuthByNameEncoder, OpsSASLAuthByName, SASLAuthByNameOptions,
};
use crate::memdx::pendingop::ClientPendingOp;
use crate::memdx::request::SASLListMechsRequest;
use crate::memdx::response::SASLListMechsResponse;
use tokio::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Credentials {
    UserPass {
        username: String,
        password: String,
    },
    #[cfg(feature = "unstable-jwt")]
    JwtToken(String),
}

impl Credentials {
    pub fn user_pass(&self) -> Result<(&str, &str)> {
        match self {
            Credentials::UserPass { username, password } => {
                Ok((username.as_str(), password.as_str()))
            }
            _ => Err(Error::new_invalid_argument_error(
                "credentials do not contain username/password",
                None,
            )),
        }
    }

    #[cfg(feature = "unstable-jwt")]
    pub fn jwt(&self) -> Result<&str> {
        match self {
            Credentials::JwtToken(token) => Ok(token.as_str()),
            _ => Err(Error::new_invalid_argument_error(
                "credentials do not contain jwt",
                None,
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthAutoOptions {
    pub credentials: Credentials,

    pub enabled_mechs: Vec<AuthMechanism>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsOptions {}

pub trait OpSASLAutoEncoder: OpSASLAuthByNameEncoder {
    fn sasl_list_mechs<D>(
        &self,
        dispatcher: &D,
        request: SASLListMechsRequest,
    ) -> impl std::future::Future<Output = Result<ClientPendingOp>>
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
            return Err(Error::new_invalid_argument_error(
                "no enabled mechanisms",
                "enabled_mechanisms".to_string(),
            ));
        }

        let mut op = encoder
            .sasl_list_mechs(dispatcher, SASLListMechsRequest {})
            .await?;
        let packet = op.recv().await?;
        let server_mechs = SASLListMechsResponse::new(packet)?.available_mechs;

        // This unwrap is safe, we know it can't be None;
        let default_mech = opts.enabled_mechs.first().unwrap();

        let by_name = OpsSASLAuthByName {};
        match by_name
            .sasl_auth_by_name(
                encoder,
                dispatcher,
                SASLAuthByNameOptions {
                    credentials: opts.credentials.clone(),
                    auth_mechanism: default_mech.clone(),
                    deadline,
                },
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                if e.is_cancellation_error() {
                    return Err(e);
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
                        return Err(Error::new_message_error("no supported mechanisms found"));
                    }
                };

                OpsSASLAuthByName {}
                    .sasl_auth_by_name(
                        encoder,
                        dispatcher,
                        SASLAuthByNameOptions {
                            credentials: opts.credentials.clone(),
                            auth_mechanism: selected_mech.clone(),
                            deadline,
                        },
                    )
                    .await
            }
        }
    }
}
