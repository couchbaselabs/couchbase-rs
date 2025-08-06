use std::fmt::Debug;

use crate::error::Result;
use crate::service_type::ServiceType;

#[derive(Debug, Clone, PartialEq, Hash)]
#[non_exhaustive]
pub enum Authenticator {
    PasswordAuthenticator(PasswordAuthenticator),
    CertificateAuthenticator(CertificateAuthenticator),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserPassPair {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
}

impl From<PasswordAuthenticator> for Authenticator {
    fn from(value: PasswordAuthenticator) -> Self {
        Authenticator::PasswordAuthenticator(value)
    }
}

// CertificateAuthenticator expects the TlsConfig provided in AgentConfig to contain the certificate chain and private key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
