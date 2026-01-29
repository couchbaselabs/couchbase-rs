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

use crate::auth_mechanism::AuthMechanism;
use crate::error::Result;
use crate::service_type::ServiceType;
use std::fmt::{Debug, Display};

#[derive(Clone, PartialEq, Hash)]
#[non_exhaustive]
pub enum Authenticator {
    PasswordAuthenticator(PasswordAuthenticator),
    CertificateAuthenticator(CertificateAuthenticator),
    #[cfg(feature = "unstable-jwt")]
    JwtAuthenticator(JwtAuthenticator),
}

impl Display for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authenticator::PasswordAuthenticator(_) => write!(f, "PasswordAuthenticator"),
            Authenticator::CertificateAuthenticator(_) => {
                write!(f, "CertificateAuthenticator")
            }
            #[cfg(feature = "unstable-jwt")]
            Authenticator::JwtAuthenticator(_) => write!(f, "JwtAuthenticator"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UserPassPair {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PasswordAuthenticator {
    pub username: String,
    pub password: String,
}

impl PasswordAuthenticator {
    pub fn get_credentials(
        &self,
        _service_type: &ServiceType,
        _host_port: String,
    ) -> Result<UserPassPair> {
        Ok(UserPassPair {
            username: self.username.clone(),
            password: self.password.clone(),
        })
    }

    pub fn get_auth_mechanisms(&self, tls_enabled: bool) -> Vec<AuthMechanism> {
        if tls_enabled {
            vec![AuthMechanism::Plain]
        } else {
            vec![
                AuthMechanism::ScramSha512,
                AuthMechanism::ScramSha256,
                AuthMechanism::ScramSha1,
            ]
        }
    }
}

impl From<PasswordAuthenticator> for Authenticator {
    fn from(value: PasswordAuthenticator) -> Self {
        Authenticator::PasswordAuthenticator(value)
    }
}

// CertificateAuthenticator expects the TlsConfig provided in AgentConfig to contain the certificate chain and private key.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CertificateAuthenticator {}

impl CertificateAuthenticator {
    pub fn get_credentials(
        &self,
        _service_type: &ServiceType,
        _host_port: String,
    ) -> Result<UserPassPair> {
        Ok(UserPassPair {
            username: String::new(), // No username for certificate auth
            password: String::new(), // No password for certificate auth
        })
    }
}

impl From<CertificateAuthenticator> for Authenticator {
    fn from(value: CertificateAuthenticator) -> Self {
        Authenticator::CertificateAuthenticator(value)
    }
}

#[cfg(feature = "unstable-jwt")]
#[derive(Clone, PartialEq, Hash)]
pub struct JwtAuthenticator {
    pub token: String,
}

#[cfg(feature = "unstable-jwt")]
impl JwtAuthenticator {
    pub fn get_token(&self) -> &str {
        &self.token
    }

    pub fn get_auth_mechanisms(&self) -> Vec<AuthMechanism> {
        vec![AuthMechanism::OAuthBearer]
    }
}

#[cfg(feature = "unstable-jwt")]
impl From<JwtAuthenticator> for Authenticator {
    fn from(value: JwtAuthenticator) -> Self {
        Authenticator::JwtAuthenticator(value)
    }
}
