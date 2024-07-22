use std::fmt::{Display, Formatter, Pointer};
use std::io;
use std::sync::Arc;

use thiserror::Error;
use tokio::time::error::Elapsed;

use crate::memdx::error;
use crate::memdx::status::Status;
use crate::scram::ScramError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    /// Taken from serde_json: This `Box` allows us to keep the size of `Error` as small as possible.
    /// A larger `Error` type was substantially slower due to all the functions
    /// that pass around `Result<T, Error>`.
    pub kind: Box<ErrorKind>,
}

#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("{0:?}")]
    Server(ServerError),
    #[error("Dispatch failed {0:?}")]
    #[non_exhaustive]
    Dispatch(Arc<io::Error>),
    #[error("Protocol error {msg}")]
    #[non_exhaustive]
    Protocol { msg: String },
    #[error("Connect error {0}")]
    Connect(Arc<io::Error>),
    #[error("Request cancelled {0}")]
    Cancelled(CancellationErrorKind),
    #[error("Connection closed")]
    Closed,
    #[error("Unknown bucket name")]
    UnknownBucketName,
    #[error("Unknown IO error {0:?}")]
    #[non_exhaustive]
    Io(Arc<io::Error>),
    #[error("Invalid argument {msg}")]
    #[non_exhaustive]
    InvalidArgument { msg: String },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("Server error: {kind}")]
#[non_exhaustive]
pub struct ServerError {
    pub kind: ServerErrorKind,
    pub config: Option<Vec<u8>>,
    pub context: Option<ServerErrorContext>,
}

impl ServerError {
    pub(crate) fn new(kind: ServerErrorKind) -> Self {
        Self {
            kind,
            config: None,
            context: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerErrorContext {
    pub text: String,
    pub error_ref: String,
    pub manifest_rev: u64,
}

#[derive(Error, Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    #[error("Not my vbucket")]
    NotMyVbucket,
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
    #[error("Unknown collection id")]
    UnknownCollectionID,
    #[error("Unknown bucket name")]
    UnknownBucketName,
    #[error("Access error")]
    Access,
    #[error("Auth error {msg}")]
    Auth { msg: String },
    #[error("Config not set")]
    ConfigNotSet,
    #[error("Server status unexpected for operation: {status}")]
    UnknownStatus { status: Status },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum CancellationErrorKind {
    Timeout,
    RequestCancelled,
    ClosedInFlight,
}

impl Error {
    pub(crate) fn new_protocol_error(msg: &str) -> Self {
        Self {
            kind: Box::new(ErrorKind::Protocol { msg: msg.into() }),
        }
    }

    pub fn has_server_config(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { config, .. }) = self.kind.as_ref() {
            config.as_ref()
        } else {
            None
        }
    }

    pub fn has_server_error_context(&self) -> Option<&ServerErrorContext> {
        if let ErrorKind::Server(ServerError { context, .. }) = self.kind.as_ref() {
            context.as_ref()
        } else {
            None
        }
    }
}

impl Display for CancellationErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            CancellationErrorKind::Timeout => "Timeout",
            CancellationErrorKind::RequestCancelled => "Request cancelled",
            CancellationErrorKind::ClosedInFlight => "Closed in flight",
        };

        write!(f, "{}", txt)
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            kind: Box::new(err.into()),
        }
    }
}

impl From<ServerErrorKind> for Error {
    fn from(kind: ServerErrorKind) -> Self {
        Self {
            kind: Box::new(ErrorKind::Server(ServerError::new(kind))),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self {
            kind: Box::new(ErrorKind::Io(Arc::new(value))),
        }
    }
}

impl From<ScramError> for Error {
    fn from(value: ScramError) -> Self {
        Self {
            kind: Box::new(ErrorKind::Server(ServerError::new(ServerErrorKind::Auth {
                msg: value.to_string(),
            }))),
        }
    }
}

impl From<Elapsed> for Error {
    fn from(_value: Elapsed) -> Self {
        Self {
            kind: Box::new(ErrorKind::Cancelled(CancellationErrorKind::Timeout)),
        }
    }
}
