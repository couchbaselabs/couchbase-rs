use crate::error::Error;
use crate::memdx;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AuthMechanism {
    Plain,
    ScramSha1,
    ScramSha256,
    ScramSha512,
}

impl From<AuthMechanism> for Vec<u8> {
    fn from(value: AuthMechanism) -> Vec<u8> {
        let txt = match value {
            AuthMechanism::Plain => "PLAIN",
            AuthMechanism::ScramSha1 => "SCRAM-SHA1",
            AuthMechanism::ScramSha256 => "SCRAM-SHA256",
            AuthMechanism::ScramSha512 => "SCRAM-SHA512",
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
            _ => {
                return Err(Error::new_invalid_argument_error(
                    format!("unsupported auth mechanism {}", value),
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
        }
    }
}
