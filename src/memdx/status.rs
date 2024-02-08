use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Status {
    Success,
    AuthError,

    Unknown,
}

impl From<u16> for Status {
    fn from(value: u16) -> Self {
        match value {
            0x00 => Status::Success,
            0x20 => Status::AuthError,
            _ => Status::Unknown,
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            Status::Success => "success",
            Status::AuthError => "authentication error",
            Status::Unknown => "unknown",
        };

        write!(f, "{}", txt)
    }
}
