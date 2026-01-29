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

use crate::memdx::error::Error;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum AuthMechanism {
    Plain,
    ScramSha1,
    ScramSha256,
    ScramSha512,
    #[cfg(feature = "unstable-jwt")]
    OAuthBearer,
}

impl From<AuthMechanism> for Vec<u8> {
    fn from(value: AuthMechanism) -> Vec<u8> {
        let txt = match value {
            AuthMechanism::Plain => "PLAIN",
            AuthMechanism::ScramSha1 => "SCRAM-SHA1",
            AuthMechanism::ScramSha256 => "SCRAM-SHA256",
            AuthMechanism::ScramSha512 => "SCRAM-SHA512",
            #[cfg(feature = "unstable-jwt")]
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
            #[cfg(feature = "unstable-jwt")]
            "OAUTHBEARER" => AuthMechanism::OAuthBearer,
            _ => {
                return Err(Error::new_protocol_error(format!(
                    "unsupported auth mechanism {value}"
                )));
            }
        };

        Ok(mech)
    }
}
