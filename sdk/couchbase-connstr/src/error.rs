use std::fmt::{Display, Formatter};
use std::{fmt, io};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Error {
    pub(crate) kind: ErrorKind,
}

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    Parse(String),
    InvalidArgument { msg: String, arg: String },
    Io(io::Error),
    Resolve(hickory_resolver::error::ResolveError),
}

impl Clone for ErrorKind {
    fn clone(&self) -> Self {
        match self {
            ErrorKind::Parse(s) => ErrorKind::Parse(s.clone()),
            ErrorKind::InvalidArgument { msg, arg } => ErrorKind::InvalidArgument {
                msg: msg.clone(),
                arg: arg.clone(),
            },
            ErrorKind::Io(e) => Self::Io(io::Error::from(e.kind())),
            ErrorKind::Resolve(e) => Self::Resolve(e.clone()),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, f)
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Parse(s) => write!(f, "Parse error: {}", s),
            ErrorKind::InvalidArgument { msg, arg } => {
                write!(f, "Invalid argument: {} ({})", msg, arg)
            }
            ErrorKind::Io(e) => write!(f, "IO error: {}", e),
            ErrorKind::Resolve(e) => write!(f, "Resolve error: {}", e),
        }
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
