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
use crate::memdx::error;
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::run_bootstrap_op_future_with_deadline;
use crate::memdx::request::SASLAuthRequest;
use crate::memdx::response::SASLAuthResponse;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLOAuthBearerOptions {
    pub token: String,
    pub deadline: tokio::time::Instant,
}

impl SASLOAuthBearerOptions {
    pub fn new(token: String, deadline: tokio::time::Instant) -> Self {
        Self { token, deadline }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLOAuthBearer {}

impl OpsSASLOAuthBearer {
    pub async fn sasl_auth_oauth_bearer<E, D>(
        &self,
        encoder: &E,
        dispatcher: &D,
        options: SASLOAuthBearerOptions,
    ) -> error::Result<()>
    where
        E: OpSASLPlainEncoder,
        D: Dispatcher,
    {
        let mut payload: Vec<u8> = Vec::new();
        payload.extend_from_slice(b"n,,");
        payload.push(1);
        payload.extend_from_slice(b"auth=Bearer ");
        payload.extend_from_slice(options.token.as_bytes());
        payload.push(1);
        payload.push(1);

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::OAuthBearer,
        };

        let resp = run_bootstrap_op_future_with_deadline(
            options.deadline,
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
