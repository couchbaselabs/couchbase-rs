
use std::fmt::{Display, Formatter};
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Dispatch failed {0}")]
    Dispatch(io::Error),
    #[error("Request cancelled {0}")]
    Cancelled(CancellationErrorKind),
    #[error("Not my vbucket")]
    NotMyVbucket,
    #[error("Protocol error {0}")]
    Protocol(String),
    #[error("Temporary failure")]
    TmpFail,
    #[error("Unknown error {0}")]
    Unknown(String),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancellationErrorKind {
    Timeout,
    RequestCancelled,
}

impl Display for CancellationErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            CancellationErrorKind::Timeout => "timeout",
            CancellationErrorKind::RequestCancelled => "request cancelled",
        };

        write!(f, "{}", txt)
    }
}

// TODO: improve this.
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Unknown(value.to_string())
    }
}
