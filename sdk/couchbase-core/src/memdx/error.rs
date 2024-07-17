use std::fmt::{Display, Formatter};
use std::io;

use tokio::time::error::Elapsed;

use crate::scram::ScramError;

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum MemdxError {
    #[error("Connect failed {0}")]
    Connect(io::ErrorKind),
    #[error("Dispatch failed {0}")]
    Dispatch(io::ErrorKind),
    #[error("Request cancelled {0}")]
    Cancelled(CancellationErrorKind),
    #[error("Not my vbucket")]
    NotMyVbucket,
    #[error("Protocol error {0}")]
    Protocol(String),
    #[error("Key exists")]
    KeyExists,
    #[error("Key not found")]
    KeyNotFound,
    #[error("Temporary failure")]
    TmpFail,
    #[error("Locked")]
    Locked,
    #[error("Too big")]
    TooBig,
    #[error("Collections not enabled")]
    CollectionsNotEnabled,
    #[error("Unknown collection id")]
    UnknownCollectionID,
    #[error("Unknown bucket name")]
    UnknownBucketName,
    #[error("Access error")]
    Access,
    #[error("Auth error")]
    Auth(String),
    #[error("Connection closed")]
    Closed,
    #[error("Config not set")]
    ConfigNotSet,
    #[error("Closed in flight")]
    ClosedInFlight,
    #[error("{0}")]
    Generic(String),
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
impl From<io::Error> for MemdxError {
    fn from(value: io::Error) -> Self {
        MemdxError::Unknown(value.to_string())
    }
}

impl From<ScramError> for MemdxError {
    fn from(value: ScramError) -> Self {
        Self::Auth(value.to_string())
    }
}

impl From<Elapsed> for MemdxError {
    fn from(_value: Elapsed) -> Self {
        Self::Cancelled(CancellationErrorKind::Timeout)
    }
}
