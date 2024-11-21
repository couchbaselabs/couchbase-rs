use crate::httpx;
use http::StatusCode;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Error {
    pub kind: Box<ErrorKind>,

    // TODO: This shouldn't be arc but I'm losing the will to live.
    pub source: Option<Arc<dyn StdError + 'static + Send + Sync>>,

    pub endpoint: String,
    pub status_code: Option<StatusCode>,
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}, caused by: {}", self.kind, source)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl Error {
    pub fn new_server_error(
        kind: ServerErrorKind,
        msg: impl Into<String>,
        endpoint: impl Into<String>,
        status_code: StatusCode,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::Server {
                kind,
                msg: msg.into(),
            }),
            endpoint: endpoint.into(),
            status_code: Some(status_code),
            source: None,
        }
    }

    pub fn new_server_error_with_source(
        kind: ServerErrorKind,
        msg: impl Into<String>,
        endpoint: impl Into<String>,
        status_code: StatusCode,
        source: Arc<dyn StdError + 'static + Send + Sync>,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::Server {
                kind,
                msg: msg.into(),
            }),
            endpoint: endpoint.into(),
            status_code: Some(status_code),
            source: Some(source),
        }
    }

    pub fn new_http_error(e: httpx::error::Error, endpoint: impl Into<String>) -> Error {
        Self {
            kind: Box::new(ErrorKind::Http(e)),
            endpoint: endpoint.into(),
            status_code: None,
            source: None,
        }
    }

    pub fn new_generic_error(msg: impl Into<String>, endpoint: impl Into<String>) -> Error {
        Self {
            kind: Box::new(ErrorKind::Generic { msg: msg.into() }),
            endpoint: endpoint.into(),
            status_code: None,
            source: None,
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    Server { kind: ServerErrorKind, msg: String },
    Http(httpx::error::Error),
    Json { msg: String },
    Generic { msg: String },
    UnsupportedFeature { feature: String },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server { kind, msg } => write!(f, "{kind} - {msg}"),
            ErrorKind::Http(e) => write!(f, "{}", e),
            ErrorKind::Json { msg } => write!(f, "{}", msg),
            ErrorKind::Generic { msg } => write!(f, "{}", msg),
            ErrorKind::UnsupportedFeature { feature } => {
                write!(f, "feature unsupported: {}", feature)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    Internal,
    AuthenticationFailure,
    IndexExists,
    IndexNotFound,
    UnknownIndexType,
    SourceTypeIncorrect,
    SourceNotFound,
    NoIndexPartitionsPlanned,
    NoIndexPartitionsFound,
    UnsupportedFeature,
    RateLimitedFailure,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::Internal => write!(f, "internal server error"),
            ServerErrorKind::AuthenticationFailure => write!(f, "authentication failure"),
            ServerErrorKind::IndexExists => write!(f, "index exists"),
            ServerErrorKind::IndexNotFound => write!(f, "index not found"),
            ServerErrorKind::UnknownIndexType => write!(f, "unknown index type"),
            ServerErrorKind::SourceTypeIncorrect => write!(f, "source type incorrect"),
            ServerErrorKind::SourceNotFound => write!(f, "source not found"),
            ServerErrorKind::NoIndexPartitionsPlanned => write!(f, "no index partitions planned"),
            ServerErrorKind::NoIndexPartitionsFound => write!(f, "no index partitions found"),
            ServerErrorKind::UnsupportedFeature => write!(f, "unsupported feature"),
            ServerErrorKind::RateLimitedFailure => write!(f, "rate limited failure"),
            ServerErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}
