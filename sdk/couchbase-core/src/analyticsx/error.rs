use http::StatusCode;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

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

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|cause| &**cause as &(dyn StdError + 'static))
    }
}

impl Error {
    pub(crate) fn new_server_error(e: ServerError) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Server(e)),
                source: None,
            },
        }
    }

    pub(crate) fn new_message_error(
        msg: impl Into<String>,
        endpoint: impl Into<Option<String>>,
        statement: impl Into<Option<String>>,
        client_context_id: impl Into<Option<String>>,
    ) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Message {
                    msg: msg.into(),
                    endpoint: endpoint.into(),
                    statement: statement.into(),
                    client_context_id: client_context_id.into(),
                }),
                source: None,
            },
        }
    }

    pub(crate) fn new_encoding_error(msg: impl Into<String>) -> Error {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Encoding { msg: msg.into() }),
                source: None,
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
                source: None,
            },
        }
    }

    pub(crate) fn new_http_error(
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<Option<String>>,
    ) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::Http {
                    endpoint: endpoint.into(),
                    statement: statement.into(),
                    client_context_id: client_context_id.into(),
                }),
                source: None,
            },
        }
    }

    pub(crate) fn new_unsupported_feature_error(feature: impl Into<String>) -> Self {
        Self {
            inner: ErrorImpl {
                kind: Box::new(ErrorKind::UnsupportedFeature {
                    feature: feature.into(),
                }),
                source: None,
            },
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    pub(crate) fn with(mut self, source: Source) -> Error {
        self.inner.source = Some(source);
        self
    }
}

impl Error {
    pub fn is_parsing_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::ParsingFailure,
                ..
            })
        )
    }

    pub fn is_internal_server_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::InternalServerError,
                ..
            })
        )
    }

    pub fn is_authentication_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::AuthenticationFailure,
                ..
            })
        )
    }

    pub fn is_compilation_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::CompilationFailure,
                ..
            })
        )
    }

    pub fn is_temporary_failure(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::TemporaryFailure,
                ..
            })
        )
    }

    pub fn is_index_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::IndexNotFound,
                ..
            })
        )
    }

    pub fn is_index_exists(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::IndexExists,
                ..
            })
        )
    }

    pub fn is_job_queue_full(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::JobQueueFull,
                ..
            })
        )
    }

    pub fn is_dataset_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DatasetNotFound,
                ..
            })
        )
    }

    pub fn is_dataverse_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DataverseNotFound,
                ..
            })
        )
    }

    pub fn is_dataset_exists(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DatasetExists,
                ..
            })
        )
    }

    pub fn is_dataverse_exists(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::DataverseExists,
                ..
            })
        )
    }

    pub fn is_link_not_found(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::LinkNotFound,
                ..
            })
        )
    }

    pub fn is_server_invalid_arg(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::ServerInvalidArg,
                ..
            })
        )
    }

    pub fn is_unknown(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Server(ServerError {
                kind: ServerErrorKind::Unknown,
                ..
            })
        )
    }
}

type Source = Arc<dyn StdError + Send + Sync>;

#[derive(Debug, Clone)]
struct ErrorImpl {
    kind: Box<ErrorKind>,
    source: Option<Source>,
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
        statement: String,
        client_context_id: Option<String>,
    },
    #[non_exhaustive]
    Message {
        msg: String,
        endpoint: Option<String>,
        statement: Option<String>,
        client_context_id: Option<String>,
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
            ErrorKind::Server(e) => write!(f, "{}", e),
            ErrorKind::InvalidArgument { msg, arg } => {
                let base_msg = format!("invalid argument: {msg}");
                if let Some(arg) = arg {
                    write!(f, "{base_msg}, arg: {arg}")
                } else {
                    write!(f, "{base_msg}")
                }
            }
            ErrorKind::Encoding { msg } => write!(f, "encoding error: {msg}"),
            ErrorKind::Http {
                endpoint,
                statement,
                client_context_id,
            } => {
                write!(
                    f,
                    "http error: endpoint: {endpoint}, statement: {statement}"
                )?;
                if let Some(client_context_id) = client_context_id {
                    write!(f, ", client context id: {client_context_id}")?;
                }
                Ok(())
            }
            ErrorKind::Message {
                msg,
                endpoint,
                statement,
                client_context_id,
            } => {
                write!(f, "{msg}")?;
                if let Some(endpoint) = endpoint {
                    write!(f, ", endpoint: {endpoint}")?;
                }
                if let Some(client_context_id) = client_context_id {
                    write!(f, ", client context id: {client_context_id}")?;
                }
                if let Some(statement) = statement {
                    write!(f, ", statement: {statement}")?;
                }
                Ok(())
            }
            ErrorKind::UnsupportedFeature { feature } => {
                write!(f, "unsupported feature: {}", feature)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerError {
    kind: ServerErrorKind,

    endpoint: String,
    status_code: StatusCode,
    code: u32,
    msg: String,

    statement: Option<String>,
    client_context_id: Option<String>,

    all_error_descs: Vec<ErrorDesc>,
}

impl ServerError {
    pub(crate) fn new(
        kind: ServerErrorKind,
        endpoint: impl Into<String>,
        status_code: StatusCode,
        code: u32,
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            endpoint: endpoint.into(),
            status_code,
            code,
            msg: msg.into(),
            statement: None,
            client_context_id: None,
            all_error_descs: vec![],
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn statement(&self) -> Option<&str> {
        self.statement.as_deref()
    }

    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn client_context_id(&self) -> Option<&str> {
        self.client_context_id.as_deref()
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn all_error_descs(&self) -> &[ErrorDesc] {
        &self.all_error_descs
    }

    pub(crate) fn with_statement(mut self, statement: impl Into<String>) -> Self {
        self.statement = Some(statement.into());
        self
    }

    pub(crate) fn with_client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    pub(crate) fn with_error_descs(mut self, error_descs: Vec<ErrorDesc>) -> Self {
        self.all_error_descs = error_descs;
        self
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "server error of kind: {} code: {}, msg: {}",
            self.kind, self.code, self.msg
        )?;

        if let Some(client_context_id) = &self.client_context_id {
            write!(f, ", client context id: {}", client_context_id)?;
        }
        if let Some(statement) = &self.statement {
            write!(f, ", statement: {}", statement)?;
        }

        write!(
            f,
            ", endpoint: {},  status code: {}",
            self.endpoint, self.status_code
        )?;

        if !self.all_error_descs.is_empty() {
            write!(f, ", all error descriptions: {:?}", self.all_error_descs)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    ParsingFailure,
    InternalServerError,
    AuthenticationFailure,
    CompilationFailure,
    TemporaryFailure,
    IndexNotFound,
    IndexExists,
    JobQueueFull,
    DatasetNotFound,
    DataverseNotFound,
    DatasetExists,
    DataverseExists,
    LinkNotFound,
    ServerInvalidArg,
    Unknown,
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::ParsingFailure => write!(f, "parsing failure"),
            ServerErrorKind::InternalServerError => write!(f, "internal server error"),
            ServerErrorKind::AuthenticationFailure => write!(f, "authentication failure"),
            ServerErrorKind::CompilationFailure => write!(f, "compilation failure"),
            ServerErrorKind::TemporaryFailure => write!(f, "temporary failure"),
            ServerErrorKind::IndexNotFound => write!(f, "index not found"),
            ServerErrorKind::IndexExists => write!(f, "index exists"),
            ServerErrorKind::JobQueueFull => write!(f, "job queue full"),
            ServerErrorKind::DatasetNotFound => write!(f, "analytics collection not found"),
            ServerErrorKind::DataverseNotFound => write!(f, "analytics scope not found"),
            ServerErrorKind::DatasetExists => write!(f, "analytics collection already exists"),
            ServerErrorKind::DataverseExists => write!(f, "analytics scope already exists"),
            ServerErrorKind::LinkNotFound => write!(f, "link not found"),
            ServerErrorKind::ServerInvalidArg => write!(f, "invalid argument"),
            ServerErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorDesc {
    kind: ServerErrorKind,

    code: u32,
    message: String,
}

impl ErrorDesc {
    pub fn new(kind: ServerErrorKind, code: u32, message: impl Into<String>) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for ErrorDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error description of kind: {}, code: {}, message: {}",
            self.kind, self.code, self.message
        )
    }
}
