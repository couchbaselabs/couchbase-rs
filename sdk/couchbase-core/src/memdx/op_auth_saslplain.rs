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

use tokio::time::Instant;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error;
use crate::memdx::error::Result;
use crate::memdx::pendingop::{run_bootstrap_op_future_with_deadline, ClientPendingOp};
use crate::memdx::request::SASLAuthRequest;
use crate::memdx::response::SASLAuthResponse;

pub trait OpSASLPlainEncoder {
    fn sasl_auth<D>(
        &self,
        dispatcher: &D,
        req: SASLAuthRequest,
    ) -> impl std::future::Future<Output = Result<ClientPendingOp>>
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
        dispatcher: &D,
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

        let resp = run_bootstrap_op_future_with_deadline(
            opts.deadline,
            encoder.sasl_auth(dispatcher, req),
        )
        .await?;

        let resp = SASLAuthResponse::new(resp)?;

        if resp.needs_more_steps {
            return Err(error::Error::new_protocol_error(
                "server did not accept auth when the client expected",
            ));
        }

        Ok(())
    }
}
