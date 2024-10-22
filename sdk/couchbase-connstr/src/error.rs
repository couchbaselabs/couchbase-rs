use std::fmt::{Display, Formatter};
use std::{fmt, io};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub struct Error {
    pub(crate) kind: ErrorKind,
}

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("failed to parse: {0}")]
    Parse(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("resolve error: {0}")]
    Resolve(#[from] hickory_resolver::error::ResolveError),
}

impl Clone for ErrorKind {
    fn clone(&self) -> Self {
        match self {
            ErrorKind::Parse(s) => ErrorKind::Parse(s.clone()),
            ErrorKind::InvalidArgument(s) => ErrorKind::InvalidArgument(s),
            ErrorKind::Io(e) => Self::from(io::Error::from(e.kind())),
            ErrorKind::Resolve(e) => Self::from(e.clone()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, f)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self {
            kind: ErrorKind::Io(e),
        }
    }
}

impl From<hickory_resolver::error::ResolveError> for Error {
    fn from(e: hickory_resolver::error::ResolveError) -> Self {
        Self {
            kind: ErrorKind::Resolve(e),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind }
    }
}
