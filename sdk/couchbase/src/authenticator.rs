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

//! Authentication types for connecting to a Couchbase cluster.
//!
//! An [`Authenticator`] must be provided when creating
//! [`ClusterOptions`](crate::options::cluster_options::ClusterOptions). The most common
//! choice is [`PasswordAuthenticator`] for RBAC username/password credentials.
//! [`CertificateAuthenticator`] is available for client-certificate (mTLS) authentication.

use std::fmt::{Debug, Display};

#[cfg(feature = "native-tls")]
use tokio_native_tls::native_tls::Identity;

#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Specifies the authentication method used to connect to a Couchbase cluster.
///
/// # Variants
///
/// - [`PasswordAuthenticator`] — RBAC username/password authentication (most common).
/// - [`CertificateAuthenticator`] — Client certificate (mTLS) authentication.
/// - [`JwtAuthenticator`] — JWT token authentication (**Uncommitted**).
///
/// # Example
///
/// ```rust
/// use couchbase::authenticator::{Authenticator, PasswordAuthenticator};
///
/// let auth: Authenticator = PasswordAuthenticator::new("user", "pass").into();
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub enum Authenticator {
    /// RBAC username/password authentication.
    PasswordAuthenticator(PasswordAuthenticator),
    /// Client certificate (mTLS) authentication.
    CertificateAuthenticator(CertificateAuthenticator),
    /// **Stability: Uncommitted** This API may change in the future.
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
            Authenticator::JwtAuthenticator(_) => write!(f, "JwtAuthenticator"),
        }
    }
}

/// Authenticates to Couchbase using RBAC username and password credentials.
///
/// This is the most common authentication method. It can be converted into an
/// [`Authenticator`] with `.into()`.
///
/// # Example
///
/// ```rust
/// use couchbase::authenticator::PasswordAuthenticator;
///
/// let auth = PasswordAuthenticator::new("Administrator", "password");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PasswordAuthenticator {
    /// The RBAC username.
    pub username: String,
    /// The RBAC password.
    pub password: String,
}

impl PasswordAuthenticator {
    /// Creates a new `PasswordAuthenticator` with the given username and password.
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

/// Authenticates to Couchbase using a client TLS certificate (mTLS).
///
/// This variant is available when the `rustls-tls` feature is enabled (and `native-tls` is not).
/// Provide a certificate chain and private key to establish mutual TLS with the cluster.
///
/// Can be converted into an [`Authenticator`] with `.into()`.
///
/// # Example
///
/// ```rust,no_run
/// use couchbase::authenticator::CertificateAuthenticator;
/// use std::fs;
///
/// // Load PEM-encoded certificate chain and private key from disk.
/// let cert_pem = fs::read("client.crt").expect("read cert");
/// let key_pem = fs::read("client.key").expect("read key");
///
/// let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
///     .collect::<Result<_, _>>()
///     .expect("parse certs");
/// let key = rustls_pemfile::private_key(&mut &key_pem[..])
///     .expect("parse key")
///     .expect("no key found");
///
/// let auth = CertificateAuthenticator::new(certs, key);
/// ```
#[cfg(all(feature = "rustls-tls", not(feature = "native-tls")))]
#[derive(Debug, PartialEq, Eq)]
pub struct CertificateAuthenticator {
    /// The client certificate chain (leaf first).
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// The client private key.
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
    /// Creates a new `CertificateAuthenticator` from a certificate chain and private key.
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

/// Authenticates to Couchbase using a client TLS certificate (mTLS).
///
/// This variant is available when the `native-tls` feature is enabled.
/// Provide a PKCS#12 [`Identity`] to establish mutual TLS with the cluster.
///
/// Can be converted into an [`Authenticator`] with `.into()`.
///
/// # Example
///
/// ```rust,no_run
/// use couchbase::authenticator::CertificateAuthenticator;
/// use tokio_native_tls::native_tls::Identity;
/// use std::fs;
///
/// // Load a PKCS#12 (.pfx / .p12) file containing both cert and key.
/// let pfx = fs::read("client.pfx").expect("read pfx");
/// let identity = Identity::from_pkcs12(&pfx, "password").expect("parse identity");
///
/// let auth = CertificateAuthenticator::new(identity);
/// ```
#[cfg(feature = "native-tls")]
#[derive(Clone)]
pub struct CertificateAuthenticator {
    /// The PKCS#12 client identity (certificate + private key).
    pub identity: Identity,
}

#[cfg(feature = "native-tls")]
impl CertificateAuthenticator {
    /// Creates a new `CertificateAuthenticator` from a PKCS#12 identity.
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

/// JwtAuthenticator uses a JWT token to authenticate with the server **Uncommitted**.
///
/// **Stability: Uncommitted** — This API may change in the future.
///
/// # Example
///
/// ```rust
/// use couchbase::authenticator::JwtAuthenticator;
///
/// let auth = JwtAuthenticator::new("eyJhbGciOiJIUzI1NiIs...");
/// ```
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JwtAuthenticator {
    /// The JWT token string.
    pub token: String,
}

impl JwtAuthenticator {
    /// Creates a new `JwtAuthenticator` with the given token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl From<JwtAuthenticator> for Authenticator {
    fn from(value: JwtAuthenticator) -> Self {
        Authenticator::JwtAuthenticator(value)
    }
}
