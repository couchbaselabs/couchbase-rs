use std::fmt::{Display, Formatter, LowerHex};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Status {
    AuthError,
    NotMyVbucket,
    Success,
    TmpFail,
    SASLAuthContinue,
    KeyExists,
    Locked,
    TooBig,
    ScopeUnknown,
    CollectionUnknown,
    AccessError,
    KeyNotFound,
    InvalidArgs,

    Unknown(u16),
}

impl From<Status> for u16 {
    fn from(value: Status) -> Self {
        match value {
            Status::Success => 0x00,
            Status::KeyNotFound => 0x01,
            Status::KeyExists => 0x02,
            Status::TooBig => 0x03,
            Status::InvalidArgs => 0x04,
            Status::Locked => 0x09,
            Status::NotMyVbucket => 0x07,
            Status::AuthError => 0x20,
            Status::SASLAuthContinue => 0x21,
            Status::AccessError => 0x24,
            Status::TmpFail => 0x86,
            Status::ScopeUnknown => 0x8c,
            Status::CollectionUnknown => 0x88,

            Status::Unknown(value) => value,
        }
    }
}

impl From<u16> for Status {
    fn from(value: u16) -> Self {
        match value {
            0x00 => Status::Success,
            0x01 => Status::KeyNotFound,
            0x02 => Status::KeyExists,
            0x03 => Status::TooBig,
            0x04 => Status::InvalidArgs,
            0x09 => Status::Locked,
            0x07 => Status::NotMyVbucket,
            0x20 => Status::AuthError,
            0x21 => Status::SASLAuthContinue,
            0x24 => Status::AccessError,
            0x86 => Status::TmpFail,
            0x8c => Status::ScopeUnknown,
            0x88 => Status::CollectionUnknown,

            _ => Status::Unknown(value),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            Status::AuthError => "authentication error",
            Status::NotMyVbucket => "not my vbucket",
            Status::Success => "success",
            Status::TmpFail => "temporary failure",
            Status::SASLAuthContinue => "authentication continue",
            Status::KeyExists => "key exists",
            Status::TooBig => "too big",
            Status::Locked => "locked",
            Status::ScopeUnknown => "scope unknown",
            Status::CollectionUnknown => "collection unknown",
            Status::AccessError => "access error",
            Status::KeyNotFound => "key not found",
            Status::InvalidArgs => "invalid args",
            Status::Unknown(status) => {
                // TODO: improve this.
                let t = format!("unknown status {}", status);

                write!(f, "{}", t)?;
                return Ok(());
            }
        };

        write!(f, "{}", txt)
    }
}
