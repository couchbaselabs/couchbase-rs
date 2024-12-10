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
    pub statement: String,
    pub client_context_id: Option<String>,

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
    pub fn new_generic_error(
        msg: impl Into<String>,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: Option<String>,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::Generic { msg: msg.into() }),
            source: None,
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id,
            status_code: None,
        }
    }

    pub fn new_generic_error_with_source(
        msg: impl Into<String>,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: Option<String>,
        source: Arc<dyn StdError + 'static + Send + Sync>,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::Generic { msg: msg.into() }),
            source: Some(source),
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id,
            status_code: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Server { error_descs: Vec<ErrorDesc> },
    Json { msg: String },
    Generic { msg: String },
    UnsupportedFeature { feature: String },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server { error_descs } => {
                write!(
                    f,
                    "{}",
                    error_descs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            }
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
    #[non_exhaustive]
    Unknown {
        msg: String,
    },
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
            ServerErrorKind::Unknown { msg } => write!(f, "unknown error: {}", msg),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorDesc {
    pub kind: Box<ServerErrorKind>,

    pub code: Option<u32>,
    pub message: Option<String>,
}

impl ErrorDesc {
    pub fn new(kind: ServerErrorKind, code: Option<u32>, message: Option<String>) -> Self {
        Self {
            kind: Box::new(kind),
            code,
            message,
        }
    }
}

impl Display for ErrorDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut base = format!("{}", self.kind);
        if let Some(code) = self.code {
            base.push_str(&format!(" (code: {})", code));
        }
        if let Some(msg) = &self.message {
            base.push_str(&format!(" - {}", msg));
        }
        write!(f, "{base}")
    }
}
