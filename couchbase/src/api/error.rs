use serde_json::Value;
use snafu::Snafu;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::{Debug, Error, Formatter};

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum CouchbaseError {
    #[snafu(display("A generic / unknown error happened: {}", ctx))]
    Generic { ctx: ErrorContext },
    #[snafu(display("Document with the given ID not found: {}", ctx))]
    DocumentNotFound { ctx: ErrorContext },
    #[snafu(display("Decoding the document content failed: {} {}", ctx, source))]
    DecodingFailure {
        ctx: ErrorContext,
        source: std::io::Error,
    },
    #[snafu(display("Encoding the document content failed: {} {}", ctx, source))]
    EncodingFailure {
        ctx: ErrorContext,
        source: std::io::Error,
    },
    #[snafu(display("The given argument is invalid: {}", ctx))]
    InvalidArgument { ctx: ErrorContext },
    #[snafu(display("The request timed out (ambiguous: {}): {}", ambiguous, ctx))]
    Timeout { ambiguous: bool, ctx: ErrorContext },
    #[snafu(display("The server reported a CAS mismatch on write: {}", ctx))]
    CasMismatch { ctx: ErrorContext },
    #[snafu(display("The request has been canceled: {}", ctx))]
    RequestCanceled { ctx: ErrorContext },
    #[snafu(display(
        "The service for this request is not available on the cluster: {}",
        ctx
    ))]
    ServiceNotAvailable { ctx: ErrorContext },
    #[snafu(display("The server experienced an internal error: {}", ctx))]
    InternalServerFailure { ctx: ErrorContext },
    #[snafu(display("Authentication was not successful: {}", ctx))]
    AuthenticationFailure { ctx: ErrorContext },
    #[snafu(display("A temporary failure occurred: {}", ctx))]
    TemporaryFailure { ctx: ErrorContext },
    #[snafu(display("Server-side parsing of the request failed: {}", ctx))]
    ParsingFailure { ctx: ErrorContext },
    #[snafu(display("The bucket is not found: {}", ctx))]
    BucketNotFound { ctx: ErrorContext },
    #[snafu(display("The collection is not found: {}", ctx))]
    CollectionNotFound { ctx: ErrorContext },
    #[snafu(display("The operation is unsupported: {}", ctx))]
    UnsupportedOperation { ctx: ErrorContext },
    #[snafu(display("The feature needed is not available on the cluster: {}", ctx))]
    FeatureNotAvailable { ctx: ErrorContext },
    #[snafu(display("The scope for this request is not found: {}", ctx))]
    ScopeNotFound { ctx: ErrorContext },
    #[snafu(display("The index is not found: {}", ctx))]
    IndexNotFound { ctx: ErrorContext },
    #[snafu(display("The index already exists: {}", ctx))]
    IndexExists { ctx: ErrorContext },
    #[snafu(display("The document is unretrievable: {}", ctx))]
    DocumentUnretrievable { ctx: ErrorContext },
    #[snafu(display("The document is locked: {}", ctx))]
    DocumentLocked { ctx: ErrorContext },
    #[snafu(display("The value is too large to be stored: {}", ctx))]
    ValueTooLarge { ctx: ErrorContext },
    #[snafu(display("The document already exists: {}", ctx))]
    DocumentExists { ctx: ErrorContext },
    #[snafu(display("The value is not JSON: {}", ctx))]
    ValueNotJson { ctx: ErrorContext },
    #[snafu(display("The durability level is (currently) not available: {}", ctx))]
    DurabilityLevelNotAvailable { ctx: ErrorContext },
    #[snafu(display("The requested durability can (currently) not be satisfied: {}", ctx))]
    DurabilityImpossible { ctx: ErrorContext },
    #[snafu(display("The server reported an ambiguous durability state: {}", ctx))]
    DurabilityAmbiguous { ctx: ErrorContext },
    #[snafu(display("Another durable write is currently in progress: {}", ctx))]
    DurableWriteInProgress { ctx: ErrorContext },
    #[snafu(display("Another durable write re-commit is currently in progress: {}", ctx))]
    DurableWriteReCommitInProgress { ctx: ErrorContext },
    #[snafu(display("The mutation has been lost: {}", ctx))]
    MutationLost { ctx: ErrorContext },
    #[snafu(display("The used path is not found: {}", ctx))]
    PathNotFound { ctx: ErrorContext },
    #[snafu(display("The used path does not match: {}", ctx))]
    PathMismatch { ctx: ErrorContext },
    #[snafu(display("The used path is invalid: {}", ctx))]
    PathInvalid { ctx: ErrorContext },
    #[snafu(display("The used path is too big: {}", ctx))]
    PathTooBig { ctx: ErrorContext },
    #[snafu(display("The used path is too deep: {}", ctx))]
    PathTooDeep { ctx: ErrorContext },
    #[snafu(display("The value is too deep: {}", ctx))]
    ValueTooDeep { ctx: ErrorContext },
    #[snafu(display("The value is invalid: {}", ctx))]
    ValueInvalid { ctx: ErrorContext },
    #[snafu(display("The requested document is not JSON: {}", ctx))]
    DocumentNotJson { ctx: ErrorContext },
    #[snafu(display("The used number is too big: {}", ctx))]
    NumberTooBig { ctx: ErrorContext },
    #[snafu(display("The provided delta (difference) is invalid: {}", ctx))]
    DeltaInvalid { ctx: ErrorContext },
    #[snafu(display("The used path already exists: {}", ctx))]
    PathExists { ctx: ErrorContext },
    #[snafu(display("Unknown XATTR macro: {}", ctx))]
    XattrUnknownMacro { ctx: ErrorContext },
    #[snafu(display("Invalid XATTR flag combination: {}", ctx))]
    XattrInvalidFlagCombo { ctx: ErrorContext },
    #[snafu(display("Invalid XATTR key combination: {}", ctx))]
    XattrInvalidKeyCombo { ctx: ErrorContext },
    #[snafu(display("Unknown XATTR virtual attribute: {}", ctx))]
    XattrUnknownVirtualAttribute { ctx: ErrorContext },
    #[snafu(display("Cannot modify XATTR virtual attribute: {}", ctx))]
    XattrCannotModifyVirtualAttribute { ctx: ErrorContext },
    #[snafu(display("Invalid XATTR ordering: {}", ctx))]
    XattrInvalidOrder { ctx: ErrorContext },
    #[snafu(display("Failed to plan the query: {}", ctx))]
    PlanningFailure { ctx: ErrorContext },
    #[snafu(display("Generic index failure: {}", ctx))]
    IndexFailure { ctx: ErrorContext },
    #[snafu(display("The prepared statement failed: {}", ctx))]
    PreparedStatementFailure { ctx: ErrorContext },
    #[snafu(display("The query could not be compilated: {}", ctx))]
    CompilationFailure { ctx: ErrorContext },
    #[snafu(display("The server job queue for this service is full: {}", ctx))]
    JobQueueFull { ctx: ErrorContext },
    #[snafu(display("The dataset is not found: {}", ctx))]
    DatasetNotFound { ctx: ErrorContext },
    #[snafu(display("The dataverse is not found: {}", ctx))]
    DataverseNotFound { ctx: ErrorContext },
    #[snafu(display("The dataset already exists: {}", ctx))]
    DatasetExists { ctx: ErrorContext },
    #[snafu(display("The dataverse already exists: {}", ctx))]
    DataverseExists { ctx: ErrorContext },
    #[snafu(display("The link is not found: {}", ctx))]
    LinkNotFound { ctx: ErrorContext },
    #[snafu(display("The view is not found: {}", ctx))]
    ViewNotFound { ctx: ErrorContext },
    #[snafu(display("The design document is not found: {}", ctx))]
    DesignDocumentNotFound { ctx: ErrorContext },
    #[snafu(display("The collection already exists: {}", ctx))]
    CollectionExists { ctx: ErrorContext },
    #[snafu(display("The scope already exists: {}", ctx))]
    ScopeExists { ctx: ErrorContext },
    #[snafu(display("The user is not found: {}", ctx))]
    UserNotFound { ctx: ErrorContext },
    #[snafu(display("The group is not found: {}", ctx))]
    GroupNotFound { ctx: ErrorContext },
    #[snafu(display("The bucket already exists: {}", ctx))]
    BucketExists { ctx: ErrorContext },
    #[snafu(display("The user already exists: {}", ctx))]
    UserExists { ctx: ErrorContext },
    #[snafu(display("The bucket does not have flush enabled: {}", ctx))]
    BucketNotFlushable { ctx: ErrorContext },
    #[snafu(display("An error occurred: {} {} {}", ctx, status, message))]
    GenericHTTP {
        ctx: ErrorContext,
        status: u16,
        message: String,
    },
}

impl CouchbaseError {
    pub(crate) fn decoding_failure_from_serde(e: serde_json::Error) -> CouchbaseError {
        CouchbaseError::DecodingFailure {
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            ctx: ErrorContext::default(),
        }
    }
    pub(crate) fn encoding_failure_from_serde(e: serde_json::Error) -> CouchbaseError {
        CouchbaseError::EncodingFailure {
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            ctx: ErrorContext::default(),
        }
    }
}

pub type CouchbaseResult<T, E = CouchbaseError> = std::result::Result<T, E>;

pub struct ErrorContext {
    inner: HashMap<String, Value>,
}

impl ErrorContext {
    pub fn insert<S: Into<String>>(&mut self, key: S, value: Value) {
        self.inner.insert(key.into(), value);
    }
}

impl Default for ErrorContext {
    fn default() -> Self {
        ErrorContext {
            inner: HashMap::new(),
        }
    }
}

impl Display for ErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}",
            serde_json::to_string(&self.inner).unwrap_or_else(|_| "".into())
        )
    }
}

impl Debug for ErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}",
            serde_json::to_string(&self.inner).unwrap_or_else(|_| "".into())
        )
    }
}
