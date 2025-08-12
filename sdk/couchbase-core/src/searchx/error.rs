use http::StatusCode;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    inner: ErrorImpl,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new_server_error(e: ServerError) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Server(e)),
            },
        }
    }

    pub(crate) fn new_message_error(
        msg: impl Into<String>,
        endpoint: impl Into<Option<String>>,
    ) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Message {
                    msg: msg.into(),
                    endpoint: endpoint.into(),
                }),
            },
        }
    }

    pub(crate) fn new_encoding_error(msg: impl Into<String>) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Encoding { msg: msg.into() }),
            },
        }
    }

    pub(crate) fn new_invalid_argument_error(
        msg: impl Into<String>,
        arg: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::InvalidArgument {
                    msg: msg.into(),
                    arg: arg.into(),
                }),
            },
        }
    }

    pub(crate) fn new_http_error(endpoint: impl Into<String>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Http {
                    endpoint: endpoint.into(),
                }),
            },
        }
    }

    pub(crate) fn new_unsupported_feature_error(feature: String) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::UnsupportedFeature { feature }),
            },
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

#[derive(Debug, Clone)]
struct ErrorImpl {
    kind: Box<ErrorKind>,
}

impl PartialEq for ErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    #[non_exhaustive]
    Http {
        endpoint: String,
    },
    #[non_exhaustive]
    Message {
        msg: String,
        endpoint: Option<String>,
    },
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    #[non_exhaustive]
    Encoding {
        msg: String,
    },
    UnsupportedFeature {
        feature: String,
    },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "{e}"),
            ErrorKind::Http { endpoint } => write!(f, "http error for endpoint {endpoint}"),
            ErrorKind::Message { msg, endpoint } => {
                if let Some(endpoint) = endpoint {
                    write!(f, "message error for endpoint {endpoint}: {msg}")
                } else {
                    write!(f, "message error: {msg}")
                }
            }
            ErrorKind::InvalidArgument { msg, arg } => {
                if let Some(arg) = arg {
                    write!(f, "invalid argument error for argument {arg}: {msg}")
                } else {
                    write!(f, "invalid argument error: {msg}")
                }
            }
            ErrorKind::Encoding { msg } => write!(f, "encoding error: {msg}"),
            ErrorKind::UnsupportedFeature { feature } => {
                write!(f, "unsupported feature: {feature}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerError {
    kind: ServerErrorKind,

    index_name: String,

    error_text: String,
    endpoint: String,
    status_code: StatusCode,
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server error for index {} at endpoint {}, status code: {}: {}, error text: {}",
            self.index_name, self.endpoint, self.status_code, self.kind, self.error_text
        )
    }
}

impl ServerError {
    pub(crate) fn new(
        kind: ServerErrorKind,
        index_name: impl Into<String>,
        error_text: impl Into<String>,
        endpoint: impl Into<String>,
        status_code: StatusCode,
    ) -> Self {
        Self {
            kind,
            error_text: error_text.into(),
            index_name: index_name.into(),
            endpoint: endpoint.into(),
            status_code,
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn error_text(&self) -> &str {
        &self.error_text
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
