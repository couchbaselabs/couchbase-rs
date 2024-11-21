use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Pointer};
use std::io;
use std::net::SocketAddr;

use serde::Deserialize;
use thiserror::Error;

use crate::memdx::error;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::status::Status;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub struct Error {
    /// Taken from serde_json: This `Box` allows us to keep the size of `Error` as small as possible.
    /// A larger `Error` type was substantially slower due to all the functions
    /// that pass around `Result<T, Error>`.
    pub kind: Box<ErrorKind>,
    pub backtrace: Backtrace,
    pub source: Option<Box<dyn StdError + 'static + Send + Sync>>,
}

impl Error {
    pub(crate) fn protocol_error(msg: impl Into<String>) -> Self {
        Self {
            kind: Box::new(ErrorKind::Protocol { msg: msg.into() }),
            backtrace: Backtrace::capture(),
            source: None,
        }
    }

    pub(crate) fn protocol_error_with_source(
        msg: impl Into<String>,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        Self {
            kind: Box::new(ErrorKind::Protocol { msg: msg.into() }),
            backtrace: Backtrace::capture(),
            source: Some(source),
        }
    }

    pub(crate) fn invalid_argument_error(msg: impl Into<String>, arg: impl Into<String>) -> Self {
        Self {
            kind: Box::new(ErrorKind::InvalidArgument {
                msg: msg.into(),
                arg: arg.into(),
            }),
            backtrace: Backtrace::capture(),
            source: None,
        }
    }

    pub(crate) fn invalid_argument_error_with_source(
        msg: impl Into<String>,
        arg: impl Into<String>,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        Self {
            kind: Box::new(ErrorKind::InvalidArgument {
                msg: msg.into(),
                arg: arg.into(),
            }),
            backtrace: Backtrace::capture(),
            source: Some(source),
        }
    }

    pub(crate) fn connection_error(
        reason: &str,
        source_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        Error {
            kind: Box::new(ErrorKind::Io {
                msg: reason.into(),
                source_addr,
                remote_addr,
            }),
            backtrace: Backtrace::capture(),
            source: Some(source),
        }
    }

    pub(crate) fn dispatch_error(
        opaque: u32,
        op_code: OpCode,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        Error {
            kind: Box::new(ErrorKind::Dispatch { opaque, op_code }),
            backtrace: Backtrace::capture(),
            source: Some(source),
        }
    }

    pub(crate) fn close_error(
        source_addr: Option<SocketAddr>,
        remote_addr: Option<SocketAddr>,
        source: Box<dyn StdError + Send + Sync>,
    ) -> Self {
        Error {
            kind: Box::new(ErrorKind::Close {
                source_addr,
                remote_addr,
            }),
            backtrace: Backtrace::capture(),
            source: Some(source),
        }
    }

    pub fn has_server_config(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { config, .. }) = self.kind.as_ref() {
            config.as_ref()
        } else {
            None
        }
    }

    pub fn has_server_error_context(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { context, .. }) = self.kind.as_ref() {
            context.as_ref()
        } else {
            None
        }
    }

    pub fn is_dispatch_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Dispatch { .. })
    }

    pub fn is_notmyvbucket_error(&self) -> bool {
        match self.kind.as_ref() {
            ErrorKind::Server(e) => e.kind == ServerErrorKind::NotMyVbucket,
            _ => false,
        }
    }

    pub fn is_unknown_collection_id_error(&self) -> bool {
        match self.kind.as_ref() {
            ErrorKind::Server(e) => e.kind == ServerErrorKind::UnknownCollectionID,
            _ => false,
        }
    }

    pub fn is_tmp_fail_error(&self) -> bool {
        match self.kind.as_ref() {
            ErrorKind::Server(e) => e.kind == ServerErrorKind::TmpFail,
            _ => false,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}, caused by: {}", self.kind, source)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        if let Some(source) = &self.source {
            Some(source.as_ref())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ErrorKind {
    Server(ServerError),
    Resource(ResourceError),
    #[non_exhaustive]
    Dispatch {
        opaque: u32,
        op_code: OpCode,
    },
    #[non_exhaustive]
    Close {
        source_addr: Option<SocketAddr>,
        remote_addr: Option<SocketAddr>,
    },
    #[non_exhaustive]
    Protocol {
        msg: String,
    },
    Cancelled(CancellationErrorKind),
    UnknownBucketName,
    #[non_exhaustive]
    Io {
        msg: String,
        source_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
    },
    #[non_exhaustive]
    UnknownIo {
        msg: String,
    },
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: String,
    },
    NoSupportedAuthMechanisms,
    #[non_exhaustive]
    Json {
        msg: String,
    },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "{}", e),
            ErrorKind::Resource(e) => write!(f, "{}", e),
            ErrorKind::Dispatch { opaque, op_code } => {
                write!(f, "Dispatch failed: opaque: {opaque}, op_code: {op_code}")
            }
            ErrorKind::Close {
                source_addr,
                remote_addr,
            } => write!(
                f,
                "Close error: source address: {}, remote: {}",
                source_addr
                    .map(|a| a.to_string())
                    .unwrap_or("unknown".to_string()),
                remote_addr
                    .map(|a| a.to_string())
                    .unwrap_or("unknown".to_string()),
            ),
            ErrorKind::Protocol { msg } => {
                write!(f, "{msg}")
            }
            ErrorKind::Cancelled(kind) => {
                write!(f, "Request cancelled: {}", kind)
            }
            ErrorKind::UnknownBucketName => write!(f, "Unknown bucket name"),
            ErrorKind::Io {
                msg: reason,
                source_addr,
                remote_addr,
            } => {
                if let Some(source_addr) = source_addr {
                    write!(
                        f,
                        "{} source address: {}, remote: {}",
                        reason, source_addr, remote_addr
                    )
                } else {
                    write!(
                        f,
                        "{} unknown source address, remote: {}",
                        reason, remote_addr
                    )
                }
            }
            ErrorKind::UnknownIo { msg } => {
                write!(f, "Unknown IO error: {msg}")
            }
            ErrorKind::InvalidArgument { msg, arg } => {
                write!(f, "invalid argument: {arg}, arg: {msg}")
            }
            ErrorKind::NoSupportedAuthMechanisms => {
                write!(f, "No supported auth mechanism was found")
            }
            ErrorKind::Json { msg } => write!(f, "Json error: {}", msg),
        }
    }
}

impl ErrorKind {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceError {
    pub cause: ServerError,
    pub scope_name: String,
    pub collection_name: Option<String>,
}

impl Display for ResourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let msg = format!(
            "resource error: {} (scope: {}, collection: {:?})",
            self.cause, self.scope_name, self.collection_name,
        );

        write!(f, "{}", msg)
    }
}

impl StdError for ResourceError {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerError {
    pub kind: ServerErrorKind,
    pub config: Option<Vec<u8>>,
    pub context: Option<Vec<u8>>,
    pub op_code: OpCode,
    pub status: Status,
    pub dispatched_to: String,
    pub dispatched_from: String,
    pub opaque: u32,
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut base_msg = format!("Server error: {}, status: 0x{:02x}, opcode: {}, dispatched from: {}, dispatched to: {}, opaque: {}",
                                   self.kind, u16::from(self.status), self.op_code, self.dispatched_from, self.dispatched_to, self.opaque);

        if let Some(context) = &self.context {
            if let Some(parsed) = Self::parse_context(context) {
                base_msg = format!("{}, (context: {})", base_msg, parsed.text);
            }
        }

        write!(f, "{}", base_msg)
    }
}

impl StdError for ServerError {}

impl ServerError {
    pub(crate) fn new(
        kind: ServerErrorKind,
        resp: &ResponsePacket,
        dispatched_to: &Option<SocketAddr>,
        dispatched_from: &Option<SocketAddr>,
    ) -> Self {
        let dispatched_to = if let Some(to) = dispatched_to {
            to.to_string()
        } else {
            String::new()
        };
        let dispatched_from = if let Some(from) = dispatched_from {
            from.to_string()
        } else {
            String::new()
        };
        Self {
            kind,
            config: None,
            context: None,
            op_code: resp.op_code,
            status: resp.status,
            dispatched_to,
            dispatched_from,
            opaque: resp.opaque,
        }
    }

    pub fn parse_context(context: &[u8]) -> Option<ServerErrorContext> {
        if context.is_empty() {
            return None;
        }

        let context_json: ServerErrorContextJson = match serde_json::from_slice(context) {
            Ok(c) => c,
            Err(_) => {
                return None;
            }
        };

        let text = context_json.error.context.unwrap_or_default();

        let error_ref = context_json.error_ref;

        let manifest_rev = context_json
            .manifest_rev
            .map(|manifest_rev| manifest_rev.parse().unwrap_or_default());

        Some(ServerErrorContext {
            text,
            error_ref,
            manifest_rev,
        })
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Default)]
struct ServerErrorContextJsonContext {
    #[serde(alias = "context")]
    context: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
struct ServerErrorContextJson {
    #[serde(alias = "text", default)]
    pub error: ServerErrorContextJsonContext,
    #[serde(alias = "ref")]
    pub error_ref: Option<String>,
    #[serde(alias = "manifest_uid")]
    pub manifest_rev: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerErrorContext {
    pub text: String,
    pub error_ref: Option<String>,
    pub manifest_rev: Option<u64>,
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
    #[error("CAS mismatch")]
    CasMismatch,
    #[error("Locked")]
    Locked,
    #[error("Not Locked")]
    NotLocked,
    #[error("Too big")]
    TooBig,
    #[error("Unknown collection id")]
    UnknownCollectionID,
    #[error("No bucket selected")]
    NoBucket,
    #[error("Unknown bucket name")]
    UnknownBucketName,
    #[error("Access error")]
    Access,
    #[error("Auth error {msg}")]
    Auth { msg: String },
    #[error("Config not set")]
    ConfigNotSet,
    #[error("scope name unknown")]
    UnknownScopeName,
    #[error("collection name unknown")]
    UnknownCollectionName,
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
            backtrace: Backtrace::capture(),
            source: None,
        }
    }
}

impl From<ServerError> for Error {
    fn from(value: ServerError) -> Self {
        Self {
            kind: Box::new(ErrorKind::Server(value)),
            backtrace: Backtrace::capture(),
            source: None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self {
            kind: Box::new(ErrorKind::UnknownIo {
                msg: value.to_string(),
            }),
            backtrace: Backtrace::capture(),
            source: Some(Box::new(value)),
        }
    }
}
