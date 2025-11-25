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

use crate::error::Error;
use crate::memdx;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AuthMechanism {
    Plain,
    ScramSha1,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

impl From<AuthMechanism> for Vec<u8> {
    fn from(value: AuthMechanism) -> Vec<u8> {
        let txt = match value {
            AuthMechanism::Plain => "PLAIN",
            AuthMechanism::ScramSha1 => "SCRAM-SHA1",
            AuthMechanism::ScramSha256 => "SCRAM-SHA256",
            AuthMechanism::ScramSha512 => "SCRAM-SHA512",
            AuthMechanism::OAuthBearer => "OAUTHBEARER",
        };

        txt.into()
    }
}

impl TryFrom<&str> for AuthMechanism {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mech = match value {
            "PLAIN" => AuthMechanism::Plain,
            "SCRAM-SHA1" => AuthMechanism::ScramSha1,
            "SCRAM-SHA256" => AuthMechanism::ScramSha256,
            "SCRAM-SHA512" => AuthMechanism::ScramSha512,
            "OAUTHBEARER" => AuthMechanism::OAuthBearer,
            _ => {
                return Err(Error::new_invalid_argument_error(
                    format!("unsupported auth mechanism {value}"),
                    None,
                ));
            }
        };

        Ok(mech)
    }
}

impl From<AuthMechanism> for memdx::auth_mechanism::AuthMechanism {
    fn from(value: AuthMechanism) -> Self {
        match value {
            AuthMechanism::Plain => memdx::auth_mechanism::AuthMechanism::Plain,
            AuthMechanism::ScramSha1 => memdx::auth_mechanism::AuthMechanism::ScramSha1,
            AuthMechanism::ScramSha256 => memdx::auth_mechanism::AuthMechanism::ScramSha256,
            AuthMechanism::ScramSha512 => memdx::auth_mechanism::AuthMechanism::ScramSha512,
            AuthMechanism::OAuthBearer => memdx::auth_mechanism::AuthMechanism::OAuthBearer,
        }
    }
}
