use serde::Deserialize;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Pointer};
use std::io;

use crate::memdx::opcode::OpCode;
use crate::memdx::status::Status;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl Error {
    pub(crate) fn new_protocol_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Protocol { msg: msg.into() },
                source: None,
            }),
        }
    }
    pub(crate) fn new_decompression_error() -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Decompression {},
                source: None,
            }),
        }
    }

    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Message(msg.into()),
                source: None,
            }),
        }
    }

    pub(crate) fn new_cancelled_error(cancellation_kind: CancellationErrorKind) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Cancelled(cancellation_kind),
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
    pub(crate) fn new_connection_failed_error(
        reason: impl Into<String>,
        source: Box<io::Error>,
    ) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::ConnectionFailed { msg: reason.into() },
                source: Some(source),
            }),
        }
    }

    pub(crate) fn new_dispatch_error(opaque: u32, op_code: OpCode, source: Box<io::Error>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Dispatch { opaque, op_code },
                source: Some(source),
            }),
        }
    }

    pub(crate) fn new_close_error(msg: String, source: Box<io::Error>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Close { msg },
                source: Some(source),
            }),
        }
    }

    pub fn has_server_config(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { config, .. }) = &self.inner.kind {
            config.as_ref()
        } else {
            None
        }
    }

    pub fn has_server_error_context(&self) -> Option<&Vec<u8>> {
        if let ErrorKind::Server(ServerError { context, .. }) = &self.inner.kind {
            context.as_ref()
        } else if let ErrorKind::Resource(ResourceError { cause, .. }) = &self.inner.kind {
            cause.context.as_ref()
        } else {
            None
        }
    }

    pub fn is_cancellation_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Cancelled { .. })
    }

    pub fn is_dispatch_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Dispatch { .. })
    }

    pub fn is_server_error_kind(&self, kind: ServerErrorKind) -> bool {
        match &self.inner.kind {
            ErrorKind::Server(e) => e.kind == kind,
            ErrorKind::Resource(e) => e.cause.kind == kind,
            _ => false,
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    pub(crate) fn with<C: Into<Source>>(mut self, source: C) -> Error {
        self.inner.source = Some(source.into());
        self
    }
}

type Source = Box<dyn StdError + Send + Sync>;

#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
    source: Option<Source>,
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

#[derive(Debug, Clone)]
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
        msg: String,
    },
    #[non_exhaustive]
    Protocol {
        msg: String,
    },
    Cancelled(CancellationErrorKind),
    #[non_exhaustive]
    ConnectionFailed {
        msg: String,
    },
    #[non_exhaustive]
    Io,
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    Decompression,
    Message(String),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Server(e) => write!(f, "{}", e),
            ErrorKind::Resource(e) => write!(f, "{}", e),
            ErrorKind::Dispatch { opaque, op_code } => {
                write!(f, "dispatch failed: opaque: {opaque}, op_code: {op_code}")
            }
            ErrorKind::Close { msg } => {
                write!(f, "close error {msg}")
            }
            ErrorKind::Protocol { msg } => {
                write!(f, "{msg}")
            }
            ErrorKind::Cancelled(kind) => {
                write!(f, "request cancelled: {kind}")
            }
            ErrorKind::ConnectionFailed { msg } => {
                write!(f, "connection failed {msg}")
            }
            ErrorKind::Io => {
                write!(f, "connection error")
            }
            ErrorKind::InvalidArgument { msg, arg } => {
                let base_msg = format!("invalid argument: {msg}");
                if let Some(arg) = arg {
                    write!(f, "{base_msg}, arg: {arg}")
                } else {
                    write!(f, "{base_msg}")
                }
            }
            ErrorKind::Decompression => write!(f, "decompression error"),
            ErrorKind::Message(msg) => write!(f, "{msg}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceError {
    cause: ServerError,
    scope_name: String,
    collection_name: String,
}

impl StdError for ResourceError {}

impl ResourceError {
    pub(crate) fn new(
        cause: ServerError,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
    ) -> Self {
        Self {
            cause,
            scope_name: scope_name.into(),
            collection_name: collection_name.into(),
        }
    }

    pub fn cause(&self) -> &ServerError {
        &self.cause
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }
}

impl Display for ResourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Resource error: {}, scope: {}, collection: {}",
            self.cause, self.scope_name, self.collection_name
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerError {
    kind: ServerErrorKind,
    config: Option<Vec<u8>>,
    context: Option<Vec<u8>>,
    op_code: OpCode,
    status: Status,
    opaque: u32,
}

impl StdError for ServerError {}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut base_msg = format!(
            "Server error: {}, status: 0x{:02x}, opcode: {}, opaque: {}",
            self.kind,
            u16::from(self.status),
            self.op_code,
            self.opaque
        );

        if let Some(context) = &self.context {
            if let Some(parsed) = Self::parse_context(context) {
                base_msg = format!("{}, (context: {})", base_msg, parsed.text);
            }
        }

        write!(f, "{}", base_msg)
    }
}

impl ServerError {
    pub(crate) fn new(kind: ServerErrorKind, op_code: OpCode, status: Status, opaque: u32) -> Self {
        Self {
            kind,
            config: None,
            context: None,
            op_code,
            status,
            opaque,
        }
    }

    pub(crate) fn with_context(mut self, context: Vec<u8>) -> Self {
        self.context = Some(context);
        self
    }

    pub(crate) fn with_config(mut self, config: Vec<u8>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn kind(&self) -> &ServerErrorKind {
        &self.kind
    }

    pub fn config(&self) -> Option<&Vec<u8>> {
        self.config.as_ref()
    }

    pub fn context(&self) -> Option<&Vec<u8>> {
        self.context.as_ref()
    }

    pub fn op_code(&self) -> OpCode {
        self.op_code
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn opaque(&self) -> u32 {
        self.opaque
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
            .map(|manifest_rev| u64::from_str_radix(&manifest_rev, 16).unwrap_or_default());

        Some(ServerErrorContext {
            text,
            error_ref,
            manifest_rev,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerErrorContext {
    pub text: String,
    pub error_ref: Option<String>,
    pub manifest_rev: Option<u64>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    NotMyVbucket,
    KeyExists,
    NotStored,
    KeyNotFound,
    TmpFail,
    CasMismatch,
    Locked,
    NotLocked,
    TooBig,
    UnknownCollectionID,
    NoBucket,
    UnknownBucketName,
    Access,
    Auth { msg: String },
    ConfigNotSet,
    UnknownScopeName,
    UnknownCollectionName,
    Subdoc { error: SubdocError },
    UnknownStatus { status: Status },
}

impl Display for ServerErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerErrorKind::NotMyVbucket => write!(f, "not my vbucket"),
            ServerErrorKind::KeyExists => write!(f, "key exists"),
            ServerErrorKind::NotStored => write!(f, "key not stored"),
            ServerErrorKind::KeyNotFound => write!(f, "key not found"),
            ServerErrorKind::TmpFail => write!(f, "temporary failure"),
            ServerErrorKind::CasMismatch => write!(f, "cas mismatch"),
            ServerErrorKind::Locked => write!(f, "locked"),
            ServerErrorKind::NotLocked => write!(f, "not locked"),
            ServerErrorKind::TooBig => write!(f, "too big"),
            ServerErrorKind::UnknownCollectionID => write!(f, "unknown collection id"),
            ServerErrorKind::NoBucket => write!(f, "no bucket selected"),
            ServerErrorKind::UnknownBucketName => write!(f, "unknown bucket name"),
            ServerErrorKind::Access => write!(f, "access error"),
            ServerErrorKind::Auth { msg } => write!(f, "auth error {}", msg),
            ServerErrorKind::ConfigNotSet => write!(f, "config not set"),
            ServerErrorKind::UnknownScopeName => write!(f, "scope name unknown"),
            ServerErrorKind::UnknownCollectionName => write!(f, "collection name unknown"),
            ServerErrorKind::Subdoc { error } => write!(f, "{}", error),
            ServerErrorKind::UnknownStatus { status } => {
                write!(f, "server status unexpected for operation: {}", status)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SubdocError {
    kind: SubdocErrorKind,
    op_index: Option<u8>,
}

impl StdError for SubdocError {}

impl SubdocError {
    pub(crate) fn new(kind: SubdocErrorKind, op_index: impl Into<Option<u8>>) -> Self {
        Self {
            kind,
            op_index: op_index.into(),
        }
    }

    pub fn is_error_kind(&self, kind: SubdocErrorKind) -> bool {
        self.kind == kind
    }

    pub fn op_index(&self) -> Option<u8> {
        self.op_index
    }
}

impl Display for SubdocError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(op_index) = self.op_index {
            let base_msg = format!("Subdoc error: {}, op_index: {}", self.kind, op_index);
            write!(f, "{}", base_msg)
        } else {
            let base_msg = format!("Subdoc error: {}", self.kind);
            write!(f, "{}", base_msg)
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SubdocErrorKind {
    PathNotFound,
    PathMismatch,
    PathInvalid,
    PathTooBig,
    DocTooDeep,
    CantInsert,
    NotJSON,
    BadRange,
    BadDelta,
    PathExists,
    ValueTooDeep,
    InvalidCombo,
    XattrInvalidFlagCombo,
    XattrInvalidKeyCombo,
    XattrUnknownMacro,
    XattrUnknownVAttr,
    XattrCannotModifyVAttr,
    InvalidXattrOrder,
    XattrUnknownVattrMacro,
    CanOnlyReviveDeletedDocuments,
    DeletedDocumentCantHaveValue,
    UnknownStatus { status: Status },
}

impl Display for SubdocErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubdocErrorKind::PathNotFound => write!(f, "subdoc path not found"),
            SubdocErrorKind::PathMismatch => write!(f, "subdoc path mismatch"),
            SubdocErrorKind::PathInvalid => write!(f, "subdoc path invalid"),
            SubdocErrorKind::PathTooBig => write!(f, "subdoc path too big"),
            SubdocErrorKind::DocTooDeep => write!(f, "subdoc doc too deep"),
            SubdocErrorKind::CantInsert => write!(f, "subdoc can't insert"),
            SubdocErrorKind::NotJSON => write!(f, "subdoc not JSON"),
            SubdocErrorKind::BadRange => write!(f, "subdoc bad range"),
            SubdocErrorKind::BadDelta => write!(f, "subdoc bad delta"),
            SubdocErrorKind::PathExists => write!(f, "subdoc path exists"),
            SubdocErrorKind::ValueTooDeep => write!(f, "subdoc value too deep"),
            SubdocErrorKind::InvalidCombo => write!(f, "subdoc invalid combo"),
            SubdocErrorKind::XattrInvalidFlagCombo => write!(f, "subdoc xattr invalid flag combo"),
            SubdocErrorKind::XattrInvalidKeyCombo => write!(f, "subdoc xattr invalid key combo"),
            SubdocErrorKind::XattrUnknownMacro => write!(f, "subdoc xattr unknown macro"),
            SubdocErrorKind::XattrUnknownVAttr => write!(f, "subdoc xattr unknown vattr"),
            SubdocErrorKind::XattrCannotModifyVAttr => {
                write!(f, "subdoc xattr cannot modify vattr")
            }
            SubdocErrorKind::InvalidXattrOrder => write!(f, "subdoc invalid xattr order"),
            SubdocErrorKind::XattrUnknownVattrMacro => {
                write!(f, "subdoc xattr unknown vattr macro")
            }
            SubdocErrorKind::CanOnlyReviveDeletedDocuments => {
                write!(f, "subdoc can only revive deleted documents")
            }
            SubdocErrorKind::DeletedDocumentCantHaveValue => {
                write!(f, "subdoc deleted document can't have value")
            }
            SubdocErrorKind::UnknownStatus { status } => write!(
                f,
                "subdoc unknown status unexpected for operation: {}",
                status
            ),
        }
    }
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

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Io,
                source: Some(Box::new(value)),
            }),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Default)]
struct ServerErrorContextJsonContext {
    #[serde(alias = "context")]
    context: Option<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
struct ServerErrorContextJson {
    #[serde(alias = "error", default)]
    error: ServerErrorContextJsonContext,
    #[serde(alias = "ref")]
    pub error_ref: Option<String>,
    #[serde(alias = "manifest_uid")]
    pub manifest_rev: Option<String>,
}
