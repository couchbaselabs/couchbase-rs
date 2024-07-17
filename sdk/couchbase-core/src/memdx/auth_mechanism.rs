use crate::memdx::error::MemdxError;

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
    type Error = MemdxError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mech = match value {
            "PLAIN" => AuthMechanism::Plain,
            "SCRAM-SHA1" => AuthMechanism::ScramSha1,
            "SCRAM-SHA256" => AuthMechanism::ScramSha256,
            "SCRAM-SHA512" => AuthMechanism::ScramSha512,
            _ => {
                return Err(MemdxError::Protocol(format!(
                    "Unknown auth mechanism {}",
                    value
                )));
            }
        };

        Ok(mech)
    }
}
