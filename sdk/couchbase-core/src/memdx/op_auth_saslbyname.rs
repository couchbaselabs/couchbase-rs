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

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslauto::Credentials;
use crate::memdx::op_auth_sasloauthbearer::{OpsSASLOAuthBearer, SASLOAuthBearerOptions};
use crate::memdx::op_auth_saslplain::{OpSASLPlainEncoder, OpsSASLAuthPlain, SASLAuthPlainOptions};
use crate::memdx::op_auth_saslscram::{OpSASLScramEncoder, OpsSASLAuthScram, SASLAuthScramOptions};
use crate::scram;
use hmac::Hmac;
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use tokio::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthByNameOptions {
    pub credentials: Credentials,

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
                let (username, password) = opts.credentials.user_pass()?;
                OpsSASLAuthPlain {}
                    .sasl_auth_plain(
                        encoder,
                        dispatcher,
                        SASLAuthPlainOptions::new(
                            username.to_string(),
                            password.to_string(),
                            opts.deadline,
                        ),
                    )
                    .await
            }
            AuthMechanism::ScramSha1 => {
                let (username, password) = opts.credentials.user_pass()?;
                OpsSASLAuthScram {}
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha1>, Sha1>::new(
                            username.to_string(),
                            password.to_string(),
                            None,
                        ),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha256 => {
                let (username, password) = opts.credentials.user_pass()?;
                OpsSASLAuthScram {}
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha256>, Sha256>::new(
                            username.to_string(),
                            password.to_string(),
                            None,
                        ),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha512 => {
                let (username, password) = opts.credentials.user_pass()?;
                OpsSASLAuthScram {}
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha512>, Sha512>::new(
                            username.to_string(),
                            password.to_string(),
                            None,
                        ),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
            AuthMechanism::OAuthBearer => {
                let token = opts.credentials.jwt()?;
                OpsSASLOAuthBearer {}
                    .sasl_auth_oauth_bearer(
                        encoder,
                        dispatcher,
                        SASLOAuthBearerOptions::new(token.to_string(), opts.deadline),
                    )
                    .await
            }
        }
    }
}
