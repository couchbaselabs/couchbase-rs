use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Message(msg.into()),
            }),
        }
    }

    pub(crate) fn new_connection_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Connection { msg: msg.into() },
            }),
        }
    }

    pub(crate) fn new_decoding_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Decoding(msg.into()),
            }),
        }
    }

    pub fn is_connection_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Connection { .. })
    }

    pub fn is_decoding_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Decoding { .. })
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorImpl {
    kind: ErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    #[non_exhaustive]
    Connection {
        msg: String,
    },
    Decoding(String),
    Message(String),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection { msg } => write!(f, "connection error {}", msg),
            Self::Decoding(msg) => write!(f, "decoding error: {}", msg),
            Self::Message(msg) => write!(f, "{}", msg),
        }
    }
}
