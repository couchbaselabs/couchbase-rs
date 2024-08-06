use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Pointer};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use tokio::time::error::Elapsed;

use crate::memdx::error;
use crate::memdx::error::ErrorKind::Server;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::status::Status;
use crate::scram::ScramError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    /// Taken from serde_json: This `Box` allows us to keep the size of `Error` as small as possible.
    /// A larger `Error` type was substantially slower due to all the functions
    /// that pass around `Result<T, Error>`.
    pub kind: Box<ErrorKind>,
}

#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("{0:?}")]
    Server(ServerError),
    #[error("{0:?}")]
    Resource(ResourceError),
    #[error("Dispatch failed {0:?}")]
    #[non_exhaustive]
    Dispatch(Arc<io::Error>),
    #[error("Protocol error {msg}")]
    #[non_exhaustive]
    Protocol { msg: String },
    #[error("Connect error {0}")]
    Connect(Arc<io::Error>),
    #[error("Request cancelled {0}")]
    Cancelled(CancellationErrorKind),
    #[error("Connection closed")]
    Closed,
    #[error("Unknown bucket name")]
    UnknownBucketName,
    #[error("Unknown IO error {0:?}")]
    #[non_exhaustive]
    Io(Arc<io::Error>),
    #[error("Invalid argument {msg}")]
    #[non_exhaustive]
    InvalidArgument { msg: String },
    #[error("No supported auth mechanism was found")]
    #[non_exhaustive]
    NoSupportedAuthMechanisms,
}

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
    #[error("Locked")]
    Locked,
    #[error("Too big")]
    TooBig,
    #[error("Unknown collection id")]
    UnknownCollectionID,
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

impl Error {
    pub(crate) fn new_protocol_error(msg: &str) -> Self {
        Self {
            kind: Box::new(ErrorKind::Protocol { msg: msg.into() }),
        }
    }

    pub fn has_server_config(&self) -> Option<&Vec<u8>> {
        if let Server(ServerError { config, .. }) = self.kind.as_ref() {
            config.as_ref()
        } else {
            None
        }
    }

    pub fn has_server_error_context(&self) -> Option<&Vec<u8>> {
        if let Server(ServerError { context, .. }) = self.kind.as_ref() {
            context.as_ref()
        } else {
            None
        }
    }

    pub fn is_dispatch_error(&self) -> bool {
        matches!(self.kind.as_ref(), ErrorKind::Dispatch(_))
    }

    pub fn is_notmyvbucket_error(&self) -> bool {
        match self.kind.as_ref() {
            Server(e) => e.kind == ServerErrorKind::NotMyVbucket,
            _ => false,
        }
    }

    pub fn is_unknown_collection_id_error(&self) -> bool {
        match self.kind.as_ref() {
            Server(e) => e.kind == ServerErrorKind::UnknownCollectionID,
            _ => false,
        }
    }
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
        }
    }
}

impl From<ServerError> for Error {
    fn from(value: ServerError) -> Self {
        Self {
            kind: Box::new(Server(value)),
        }
    }
}

impl From<ScramError> for Error {
    fn from(e: ScramError) -> Self {
        ErrorKind::Protocol { msg: e.to_string() }.into()
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self {
            kind: Box::new(ErrorKind::Io(Arc::new(value))),
        }
    }
}

impl From<Elapsed> for Error {
    fn from(_value: Elapsed) -> Self {
        Self {
            kind: Box::new(ErrorKind::Cancelled(CancellationErrorKind::Timeout)),
        }
    }
}
