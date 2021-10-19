use std::fmt::Debug;

// Internal: Do not implement.
// The only supported implementations of Authenticator are PasswordAuthenticator and
// CertificateAuthenticator.
pub trait Authenticator: Debug {
    fn username(&self) -> Option<&String>;
    fn password(&self) -> Option<&String>;
    fn certificate_path(&self) -> Option<&String>;
    fn key_path(&self) -> Option<&String>;
}

#[derive(Debug, Clone)]
pub struct PasswordAuthenticator {
    username: String,
    password: String,
}

impl PasswordAuthenticator {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl Authenticator for PasswordAuthenticator {
    fn username(&self) -> Option<&String> {
        Some(&self.username)
    }

    fn password(&self) -> Option<&String> {
        Some(&self.password)
    }

    fn certificate_path(&self) -> Option<&String> {
        None
    }

    fn key_path(&self) -> Option<&String> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct CertificateAuthenticator {
    cert_path: String,
    key_path: String,
}

impl CertificateAuthenticator {
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }
}

impl Authenticator for CertificateAuthenticator {
    fn username(&self) -> Option<&String> {
        None
    }

    fn password(&self) -> Option<&String> {
        None
    }

    fn certificate_path(&self) -> Option<&String> {
        Some(&self.cert_path)
    }

    fn key_path(&self) -> Option<&String> {
        Some(&self.key_path)
    }
}
