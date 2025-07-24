use crate::service_type::ServiceType;

#[derive(Debug, Clone, PartialEq, Hash)]
#[non_exhaustive]
pub enum Authenticator {
    PasswordAuthenticator(PasswordAuthenticator),
    // TODO: get_client_certificate needs some thought about how to expose the certificate
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
    ) -> crate::error::Result<UserPassPair> {
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
        }
    }
}
