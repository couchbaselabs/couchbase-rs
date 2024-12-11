use http::StatusCode;
use std::error::Error as StdError;
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub struct Error {
    pub kind: Box<ErrorKind>,

    pub source: Option<Box<dyn StdError + 'static + Send + Sync>>,
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}, caused by: {}", self.kind, source)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

#[derive(Debug, Clone)]
pub enum ErrorKind {
    #[non_exhaustive]
    Server {
        status_code: StatusCode,
        body: String,
        kind: ServerErrorKind,
    },
    #[non_exhaustive]
    Generic { msg: String },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server {
                status_code,
                body,
                kind,
            } => write!(
                f,
                "server error: status code: {}, body: {}, kind: {}",
                status_code, body, kind
            ),
            ErrorKind::Generic { msg } => write!(f, "generic error: {}", msg),
        }
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum ServerErrorKind {
    AccessDenied,
    UnsupportedFeature,
    ScopeExists,
    ScopeNotFound,
    CollectionExists,
    CollectionNotFound,
    BucketExists,
    BucketNotFound,
    FlushDisabled,
    ServerInvalidArg,
    BucketUuidMismatch,
    UserNotFound,
    OperationDelayed,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::AccessDenied => write!(f, "access denied"),
            ServerErrorKind::UnsupportedFeature => write!(f, "unsupported feature"),
            ServerErrorKind::ScopeExists => write!(f, "scope exists"),
            ServerErrorKind::ScopeNotFound => write!(f, "scope not found"),
            ServerErrorKind::CollectionExists => write!(f, "collection exists"),
            ServerErrorKind::CollectionNotFound => write!(f, "collection not found"),
            ServerErrorKind::BucketExists => write!(f, "bucket exists"),
            ServerErrorKind::BucketNotFound => write!(f, "bucket not found"),
            ServerErrorKind::FlushDisabled => write!(f, "flush disabled"),
            ServerErrorKind::ServerInvalidArg => write!(f, "server invalid argument"),
            ServerErrorKind::BucketUuidMismatch => write!(f, "bucket uuid mismatch"),
            ServerErrorKind::UserNotFound => write!(f, "user not found"),
            ServerErrorKind::OperationDelayed => {
                write!(f, "operation was delayed, but will continue")
            }
            ServerErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}
