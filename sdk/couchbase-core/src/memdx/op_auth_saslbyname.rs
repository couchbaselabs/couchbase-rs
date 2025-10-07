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

use hmac::Hmac;
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Result;
use crate::memdx::op_auth_saslplain::{OpSASLPlainEncoder, OpsSASLAuthPlain, SASLAuthPlainOptions};
use crate::memdx::op_auth_saslscram::{OpSASLScramEncoder, OpsSASLAuthScram, SASLAuthScramOptions};
use crate::scram;

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
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha1>, Sha1>::new(opts.username, opts.password, None),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha256 => {
                OpsSASLAuthScram {}
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha256>, Sha256>::new(
                            opts.username,
                            opts.password,
                            None,
                        ),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
            AuthMechanism::ScramSha512 => {
                OpsSASLAuthScram {}
                    .sasl_auth_scram(
                        encoder,
                        dispatcher,
                        scram::Client::<Hmac<Sha512>, Sha512>::new(
                            opts.username,
                            opts.password,
                            None,
                        ),
                        SASLAuthScramOptions::new(opts.deadline),
                    )
                    .await
            }
        }
    }
}
