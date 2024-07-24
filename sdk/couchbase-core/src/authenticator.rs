use std::fmt::Debug;

use crate::error::Result;
use crate::service_type::ServiceType;

pub trait Authenticator: Debug + Send + Sync {
    // TODO: get_client_certificate needs some thought about how to expose the certificate
    // fn get_client_certificate(service: ServiceType, host_port: String) ->
    fn get_credentials(&self, service_type: ServiceType, host_port: String)
        -> Result<UserPassPair>;
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

impl Authenticator for PasswordAuthenticator {
    fn get_credentials(
        &self,
        _service_type: ServiceType,
        _host_port: String,
    ) -> Result<UserPassPair> {
        Ok(UserPassPair {
            username: self.username.clone(),
            password: self.password.clone(),
        })
    }
}
