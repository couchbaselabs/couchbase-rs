use http::{Method, StatusCode};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|cause| &**cause as &(dyn StdError + 'static))
    }
}

impl Error {
    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Message(msg.into()),
                source: None,
            }),
        }
    }

    pub(crate) fn new_invalid_argument_error(
        msg: impl Into<String>,
        arg: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::InvalidArgument {
                    msg: msg.into(),
                    arg: arg.into(),
                },
                source: None,
            }),
        }
    }

    pub(crate) fn with<C: Into<Source>>(mut self, source: C) -> Error {
        self.inner.source = Some(source.into());
        self
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

type Source = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
    source: Option<Source>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    Resource(ResourceError),
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    Message(String),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "server error: {}", e),
            ErrorKind::Resource(e) => write!(f, "resource error: {}", e),
            ErrorKind::InvalidArgument { msg, arg } => {
                if let Some(arg) = arg {
                    write!(f, "invalid argument: {}: {}", msg, arg)
                } else {
                    write!(f, "invalid argument: {}", msg)
                }
            }
            ErrorKind::Message(msg) => write!(f, "{}", msg),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    status_code: StatusCode,
    url: String,
    body: String,
    method: Method,
    path: String,
    kind: ServerErrorKind,
}

impl StdError for ServerError {}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server error: method: {}, path: {} status code: {}, body: {}, kind: {}",
            self.method, self.path, self.status_code, self.body, self.kind
        )
    }
}

impl ServerError {
    pub(crate) fn new(
        status_code: StatusCode,
        url: String,
        method: Method,
        path: String,
        body: String,
        kind: ServerErrorKind,
    ) -> Self {
        Self {
            status_code,
            url,
            method,
            path,
            body,
            kind,
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    AccessDenied,
    UnsupportedFeature { feature: String },
    ScopeExists,
    ScopeNotFound,
    CollectionExists,
    CollectionNotFound,
    BucketExists,
    BucketNotFound,
    FlushDisabled,
    ServerInvalidArg { arg: String, reason: String },
    BucketUuidMismatch,
    UserNotFound,
    GroupNotFound,
    OperationDelayed,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::AccessDenied => write!(f, "access denied"),
            ServerErrorKind::UnsupportedFeature { feature } => {
                write!(f, "unsupported feature {}", feature)
            }
            ServerErrorKind::ScopeExists => write!(f, "scope exists"),
            ServerErrorKind::ScopeNotFound => write!(f, "scope not found"),
            ServerErrorKind::CollectionExists => write!(f, "collection exists"),
            ServerErrorKind::CollectionNotFound => write!(f, "collection not found"),
            ServerErrorKind::BucketExists => write!(f, "bucket exists"),
            ServerErrorKind::BucketNotFound => write!(f, "bucket not found"),
            ServerErrorKind::FlushDisabled => write!(f, "flush disabled"),
            ServerErrorKind::ServerInvalidArg { arg, reason } => {
                write!(f, "server invalid argument: {} - {}", arg, reason)
            }
            ServerErrorKind::BucketUuidMismatch => write!(f, "bucket uuid mismatch"),
            ServerErrorKind::UserNotFound => write!(f, "user not found"),
            ServerErrorKind::GroupNotFound => write!(f, "group not found"),
            ServerErrorKind::OperationDelayed => {
                write!(f, "operation was delayed, but will continue")
            }
            ServerErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceError {
    cause: ServerError,
    scope_name: String,
    collection_name: String,
    bucket_name: String,
}

impl StdError for ResourceError {}

impl Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resource error: scope: {}, collection: {}, bucket: {}, cause: {}",
            self.scope_name, self.collection_name, self.bucket_name, self.cause
        )
    }
}

impl ResourceError {
    pub(crate) fn new(
        cause: ServerError,
        bucket_name: impl Into<String>,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            cause,
            bucket_name: bucket_name.into(),
            scope_name: scope_name.into(),
            collection_name: collection_name.into(),
        }
    }

    pub fn cause(&self) -> &ServerError {
        &self.cause
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::from(err),
                source: None,
            }),
        }
    }
}

impl From<ServerError> for Error {
    fn from(value: ServerError) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Server(value),
                source: None,
            }),
        }
    }
}

impl From<ResourceError> for Error {
    fn from(value: ResourceError) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Resource(value),
                source: None,
            }),
        }
    }
}
