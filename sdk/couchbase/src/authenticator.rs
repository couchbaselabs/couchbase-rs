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

use std::fmt::{Debug, Display};

#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Identity;

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

#[derive(Clone)]
#[non_exhaustive]
pub enum Authenticator {
    PasswordAuthenticator(PasswordAuthenticator),
    CertificateAuthenticator(CertificateAuthenticator),
    #[cfg(feature = "unstable-jwt")]
    JwtAuthenticator(JwtAuthenticator),
}

impl Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authenticator::PasswordAuthenticator(_) => {
                write!(f, "PasswordAuthenticator")
            }
            Authenticator::CertificateAuthenticator(_) => {
                write!(f, "CertificateAuthenticator")
            }
            #[cfg(feature = "unstable-jwt")]
            Authenticator::JwtAuthenticator(_) => {
                write!(f, "JwtAuthenticator")
            }
        }
    }
}

impl Display for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authenticator::PasswordAuthenticator(_) => write!(f, "PasswordAuthenticator"),
            Authenticator::CertificateAuthenticator(_) => write!(f, "CertificateAuthenticator"),
            #[cfg(feature = "unstable-jwt")]
            Authenticator::JwtAuthenticator(_) => write!(f, "JwtAuthenticator"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PasswordAuthenticator {
    pub username: String,
    pub password: String,
}

impl PasswordAuthenticator {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl From<PasswordAuthenticator> for Authenticator {
    fn from(value: PasswordAuthenticator) -> Self {
        Authenticator::PasswordAuthenticator(value)
    }
}

impl From<Authenticator> for couchbase_core::authenticator::Authenticator {
    fn from(authenticator: Authenticator) -> Self {
        match authenticator {
            Authenticator::PasswordAuthenticator(pwd_auth) => {
                couchbase_core::authenticator::Authenticator::PasswordAuthenticator(
                    couchbase_core::authenticator::PasswordAuthenticator {
                        username: pwd_auth.username,
                        password: pwd_auth.password,
                    },
                )
            }
            Authenticator::CertificateAuthenticator(_) => {
                couchbase_core::authenticator::Authenticator::CertificateAuthenticator(
                    couchbase_core::authenticator::CertificateAuthenticator {},
                )
            }
            #[cfg(feature = "unstable-jwt")]
            Authenticator::JwtAuthenticator(jwt_auth) => {
                couchbase_core::authenticator::Authenticator::JwtAuthenticator(
                    couchbase_core::authenticator::JwtAuthenticator {
                        token: jwt_auth.token,
                    },
                )
            }
        }
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
#[derive(Debug, PartialEq, Eq)]
pub struct CertificateAuthenticator {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl Clone for CertificateAuthenticator {
    fn clone(&self) -> Self {
        Self {
            cert_chain: self.cert_chain.clone(),
            private_key: self.private_key.clone_key(),
        }
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl CertificateAuthenticator {
    pub fn new(
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Self {
        Self {
            cert_chain,
            private_key,
        }
    }
}

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
impl From<CertificateAuthenticator> for Authenticator {
    fn from(value: CertificateAuthenticator) -> Self {
        Authenticator::CertificateAuthenticator(value)
    }
}

#[cfg(feature = "native-tls")]
#[derive(Clone)]
pub struct CertificateAuthenticator {
    pub identity: Identity,
}

#[cfg(feature = "native-tls")]
impl CertificateAuthenticator {
    pub fn new(identity: Identity) -> Self {
        Self { identity }
    }
}

#[cfg(feature = "native-tls")]
impl From<CertificateAuthenticator> for Authenticator {
    fn from(value: CertificateAuthenticator) -> Self {
        Authenticator::CertificateAuthenticator(value)
    }
}

#[cfg(feature = "unstable-jwt")]
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JwtAuthenticator {
    pub token: String,
}

#[cfg(feature = "unstable-jwt")]
impl JwtAuthenticator {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}
