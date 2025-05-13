use std::fmt::{Display, Formatter, LowerHex};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Status {
    // Success indicates the operation completed successfully.
    Success,
    // KeyNotFound occurs when an operation is performed on a key that does not exist.
    KeyNotFound,
    // KeyExists occurs when an operation is performed on a key that could not be found.
    KeyExists,
    // TooBig occurs when an operation attempts to store more data in a single document
    // than the server is capable of storing (by default, this is a 20MB limit).
    TooBig,
    // InvalidArgs occurs when the server receives invalid arguments for an operation.
    InvalidArgs,
    // NotStored occurs when the server fails to store a key.
    NotStored,
    // BadDelta occurs when performing a counter op and the document is non-numeric.
    BadDelta,
    // NotMyVBucket occurs when an operation is dispatched to a server which is
    // non-authoritative for a specific vbucket.
    NotMyVbucket,
    // NoBucket occurs when no bucket was selected on a connection.
    NoBucket,
    // Locked occurs when an operation fails due to the document being locked.
    Locked,
    // OpaqueNoMatch occurs when the opaque does not match a known stream.
    OpaqueNoMatch,
    // WouldThrottle indicates that the operation would have been throttled.
    WouldThrottle,
    // ConfigOnly occurs when a data operation is performed on a config-only node.
    ConfigOnly,
    // NotLocked occurs when Unlock is performed on an unlocked document.
    // Added in 7.6.0 under MB-58088.
    NotLocked,
    // AuthStale occurs when authentication credentials have become invalidated.
    AuthStale,
    // AuthError occurs when the authentication information provided was not valid.
    AuthError,
    // AuthContinue occurs in multi-step authentication when more authentication
    // work needs to be performed in order to complete the authentication process.
    AuthContinue,
    // RangeError occurs when the range specified to the server is not valid.
    RangeError,
    // AccessError occurs when an access error occurs.
    AccessError,
    // NotInitialized is sent by servers which are still initializing, and are not
    // yet ready to accept operations on behalf of a particular bucket.
    NotInitialized,
    // RateLimitedNetworkIngress occurs when the server rate limits due to network ingress.
    RateLimitedNetworkIngress,
    // RateLimitedNetworkEgress occurs when the server rate limits due to network egress.
    RateLimitedNetworkEgress,
    // RateLimitedMaxConnections occurs when the server rate limits due to the application reaching the maximum
    // number of allowed connections.
    RateLimitedMaxConnections,
    // RateLimitedMaxCommands occurs when the server rate limits due to the application reaching the maximum
    // number of allowed operations.
    RateLimitedMaxCommands,
    // RateLimitedScopeSizeLimitExceeded occurs when the server rate limits due to the application reaching the maximum
    // data size allowed for the scope.
    RateLimitedScopeSizeLimitExceeded,
    // UnknownCommand occurs when an unknown operation is sent to a server.
    CommandUnknown,
    // OutOfMemory occurs when the server cannot service a request due to memory
    // limitations.
    OutOfMemory,
    // NotSupported occurs when an operation is understood by the server, but that
    // operation is not supported on this server (occurs for a variety of reasons).
    NotSupported,
    // InternalError occurs when internal errors prevent the server from processing
    // your request.
    InternalError,
    // Busy occurs when the server is too busy to process your request right away.
    // Attempting the operation at a later time will likely succeed.
    Busy,
    // TmpFail occurs when a temporary failure is preventing the server from
    // processing your request.
    TmpFail,
    // CollectionUnknown occurs when a Collection cannot be found.
    CollectionUnknown,
    // ScopeUnknown occurs when a Scope cannot be found.
    ScopeUnknown,
    // DurabilityInvalidLevel occurs when an invalid durability level was requested.
    DurabilityInvalidLevel,
    // DurabilityImpossible occurs when a request is performed with impossible
    // durability level requirements.
    DurabilityImpossible,
    // SyncWriteInProgress occurs when an attempt is made to write to a key that has
    // a SyncWrite pending.
    SyncWriteInProgress,
    // SyncWriteAmbiguous occurs when an SyncWrite does not complete in the specified
    // time and the result is ambiguous.
    SyncWriteAmbiguous,
    // SyncWriteReCommitInProgress occurs when an SyncWrite is being recommitted.
    SyncWriteRecommitInProgress,
    // RangeScanCancelled occurs during a range scan to indicate that the range scan was cancelled.
    RangeScanCancelled,
    // RangeScanMore occurs during a range scan to indicate that a range scan has more results.
    RangeScanMore,
    // RangeScanComplete occurs during a range scan to indicate that a range scan has completed.
    RangeScanComplete,
    // RangeScanVbUUIDNotEqual occurs during a range scan to indicate that a vb-uuid mismatch has occurred.
    RangeScanVBUUIDNotEqual,
    // SubDocPathNotFound occurs when a sub-document operation targets a path
    // which does not exist in the specifie document.
    SubDocPathNotFound,
    // SubDocPathMismatch occurs when a sub-document operation specifies a path
    // which does not match the document structure (field access on an array).
    SubDocPathMismatch,
    // SubDocPathInvalid occurs when a sub-document path could not be parsed.
    SubDocPathInvalid,
    // SubDocPathTooBig occurs when a sub-document path is too big.
    SubDocPathTooBig,
    // SubDocDocTooDeep occurs when an operation would cause a document to be
    // nested beyond the depth limits allowed by the sub-document specification.
    SubDocDocTooDeep,
    // SubDocCantInsert occurs when a sub-document operation could not insert.
    SubDocCantInsert,
    // SubDocNotJSON occurs when a sub-document operation is performed on a
    // document which is not JSON.
    SubDocNotJSON,
    // SubDocBadRange occurs when a sub-document operation is performed with
    // a bad range.
    SubDocBadRange,
    // SubDocBadDelta occurs when a sub-document counter operation is performed
    // and the specified delta is not valid.
    SubDocBadDelta,
    // SubDocPathExists occurs when a sub-document operation expects a path not
    // to exists, but the path was found in the document.
    SubDocPathExists,
    // SubDocValueTooDeep occurs when a sub-document operation specifies a value
    // which is deeper than the depth limits of the sub-document specification.
    SubDocValueTooDeep,
    // SubDocInvalidCombo occurs when a multi-operation sub-document operation is
    // performed and operations within the package of ops conflict with each other.
    SubDocInvalidCombo,
    // SubDocMultiPathFailure occurs when a multi-operation sub-document operation is
    // performed and operations within the package of ops conflict with each other.
    SubDocMultiPathFailure,
    // SubDocSuccessDeleted occurs when a multi-operation sub-document operation
    // is performed on a soft-deleted document.
    SubDocSuccessDeleted,
    // SubDocXattrInvalidFlagCombo occurs when an invalid set of
    // extended-attribute flags is passed to a sub-document operation.
    SubDocXattrInvalidFlagCombo,
    // SubDocXattrInvalidKeyCombo occurs when an invalid set of key operations
    // are specified for a extended-attribute sub-document operation.
    SubDocXattrInvalidKeyCombo,
    // SubDocXattrUnknownMacro occurs when an invalid macro value is specified.
    SubDocXattrUnknownMacro,
    // SubDocXattrUnknownVAttr occurs when an invalid virtual attribute is specified.
    SubDocXattrUnknownVAttr,
    // SubDocXattrCannotModifyVAttr occurs when a mutation is attempted upon
    // a virtual attribute (which are immutable by definition).
    SubDocXattrCannotModifyVAttr,
    // SubDocMultiPathFailureDeleted occurs when a Multi Path Failure occurs on
    // a soft-deleted document.
    SubDocMultiPathFailureDeleted,
    // SubDocInvalidXattrOrder occurs when xattr operations exist after non-xattr
    // operations in the operation list.
    SubDocInvalidXattrOrder,
    // SubDocXattrUnknownVattrMacro occurs when you try to use an unknown vattr.
    SubDocXattrUnknownVattrMacro,
    // SubDocCanOnlyReviveDeletedDocuments occurs when you try to revive a document
    // which is not currently in the soft-deleted state.
    SubDocCanOnlyReviveDeletedDocuments,
    // SubDocDeletedDocumentCantHaveValue occurs when you try set a value to a
    // soft-deleted document.
    SubDocDeletedDocumentCantHaveValue,

    Unknown(u16),
}

impl From<Status> for u16 {
    fn from(value: Status) -> Self {
        Self::from(&value)
    }
}

impl From<&Status> for u16 {
    fn from(value: &Status) -> Self {
        match value {
            Status::Success => 0x00,
            Status::KeyNotFound => 0x01,
            Status::KeyExists => 0x02,
            Status::TooBig => 0x03,
            Status::InvalidArgs => 0x04,
            Status::NotStored => 0x05,
            Status::BadDelta => 0x06,
            Status::NotMyVbucket => 0x07,
            Status::NoBucket => 0x08,
            Status::Locked => 0x09,
            Status::OpaqueNoMatch => 0x0b,
            Status::WouldThrottle => 0x0c,
            Status::ConfigOnly => 0x0d,
            Status::NotLocked => 0x0e,
            Status::AuthStale => 0x1f,
            Status::AuthError => 0x20,
            Status::AuthContinue => 0x21,
            Status::RangeError => 0x22,
            Status::AccessError => 0x24,
            Status::NotInitialized => 0x25,
            Status::RateLimitedNetworkIngress => 0x30,
            Status::RateLimitedNetworkEgress => 0x31,
            Status::RateLimitedMaxConnections => 0x32,
            Status::RateLimitedMaxCommands => 0x33,
            Status::RateLimitedScopeSizeLimitExceeded => 0x34,
            Status::CommandUnknown => 0x81,
            Status::OutOfMemory => 0x82,
            Status::NotSupported => 0x83,
            Status::InternalError => 0x84,
            Status::Busy => 0x85,
            Status::TmpFail => 0x86,
            Status::CollectionUnknown => 0x88,
            Status::ScopeUnknown => 0x8c,
            Status::DurabilityInvalidLevel => 0xa0,
            Status::DurabilityImpossible => 0xa1,
            Status::SyncWriteInProgress => 0xa2,
            Status::SyncWriteAmbiguous => 0xa3,
            Status::SyncWriteRecommitInProgress => 0xa4,
            Status::RangeScanCancelled => 0xa5,
            Status::RangeScanMore => 0xa6,
            Status::RangeScanComplete => 0xa7,
            Status::RangeScanVBUUIDNotEqual => 0xa8,
            Status::SubDocPathNotFound => 0xc0,
            Status::SubDocPathMismatch => 0xc1,
            Status::SubDocPathInvalid => 0xc2,
            Status::SubDocPathTooBig => 0xc3,
            Status::SubDocDocTooDeep => 0xc4,
            Status::SubDocCantInsert => 0xc5,
            Status::SubDocNotJSON => 0xc6,
            Status::SubDocBadRange => 0xc7,
            Status::SubDocBadDelta => 0xc8,
            Status::SubDocPathExists => 0xc9,
            Status::SubDocValueTooDeep => 0xca,
            Status::SubDocInvalidCombo => 0xcb,
            Status::SubDocMultiPathFailure => 0xcc,
            Status::SubDocSuccessDeleted => 0xcd,
            Status::SubDocXattrInvalidFlagCombo => 0xce,
            Status::SubDocXattrInvalidKeyCombo => 0xcf,
            Status::SubDocXattrUnknownMacro => 0xd0,
            Status::SubDocXattrUnknownVAttr => 0xd1,
            Status::SubDocXattrCannotModifyVAttr => 0xd2,
            Status::SubDocMultiPathFailureDeleted => 0xd3,
            Status::SubDocInvalidXattrOrder => 0xd4,
            Status::SubDocXattrUnknownVattrMacro => 0xd5,
            Status::SubDocCanOnlyReviveDeletedDocuments => 0xd6,
            Status::SubDocDeletedDocumentCantHaveValue => 0xd7,

            Status::Unknown(value) => *value,
        }
    }
}

impl From<u16> for Status {
    fn from(value: u16) -> Self {
        match value {
            0x00 => Status::Success,
            0x01 => Status::KeyNotFound,
            0x02 => Status::KeyExists,
            0x03 => Status::TooBig,
            0x04 => Status::InvalidArgs,
            0x05 => Status::NotStored,
            0x06 => Status::BadDelta,
            0x07 => Status::NotMyVbucket,
            0x08 => Status::NoBucket,
            0x09 => Status::Locked,
            0x0b => Status::OpaqueNoMatch,
            0x0c => Status::WouldThrottle,
            0x0d => Status::ConfigOnly,
            0x0e => Status::NotLocked,
            0x1f => Status::AuthStale,
            0x20 => Status::AuthError,
            0x21 => Status::AuthContinue,
            0x22 => Status::RangeError,
            0x24 => Status::AccessError,
            0x25 => Status::NotInitialized,
            0x30 => Status::RateLimitedNetworkIngress,
            0x31 => Status::RateLimitedNetworkEgress,
            0x32 => Status::RateLimitedMaxConnections,
            0x33 => Status::RateLimitedMaxCommands,
            0x34 => Status::RateLimitedScopeSizeLimitExceeded,
            0x81 => Status::CommandUnknown,
            0x82 => Status::OutOfMemory,
            0x83 => Status::NotSupported,
            0x84 => Status::InternalError,
            0x85 => Status::Busy,
            0x86 => Status::TmpFail,
            0x88 => Status::CollectionUnknown,
            0x8c => Status::ScopeUnknown,
            0xa0 => Status::DurabilityInvalidLevel,
            0xa1 => Status::DurabilityImpossible,
            0xa2 => Status::SyncWriteInProgress,
            0xa3 => Status::SyncWriteAmbiguous,
            0xa4 => Status::SyncWriteRecommitInProgress,
            0xa5 => Status::RangeScanCancelled,
            0xa6 => Status::RangeScanMore,
            0xa7 => Status::RangeScanComplete,
            0xa8 => Status::RangeScanVBUUIDNotEqual,
            0xc0 => Status::SubDocPathNotFound,
            0xc1 => Status::SubDocPathMismatch,
            0xc2 => Status::SubDocPathInvalid,
            0xc3 => Status::SubDocPathTooBig,
            0xc4 => Status::SubDocDocTooDeep,
            0xc5 => Status::SubDocCantInsert,
            0xc6 => Status::SubDocNotJSON,
            0xc7 => Status::SubDocBadRange,
            0xc8 => Status::SubDocBadDelta,
            0xc9 => Status::SubDocPathExists,
            0xca => Status::SubDocValueTooDeep,
            0xcb => Status::SubDocInvalidCombo,
            0xcc => Status::SubDocMultiPathFailure,
            0xcd => Status::SubDocSuccessDeleted,
            0xce => Status::SubDocXattrInvalidFlagCombo,
            0xcf => Status::SubDocXattrInvalidKeyCombo,
            0xd0 => Status::SubDocXattrUnknownMacro,
            0xd1 => Status::SubDocXattrUnknownVAttr,
            0xd2 => Status::SubDocXattrCannotModifyVAttr,
            0xd3 => Status::SubDocMultiPathFailureDeleted,
            0xd4 => Status::SubDocInvalidXattrOrder,
            0xd5 => Status::SubDocXattrUnknownVattrMacro,
            0xd6 => Status::SubDocCanOnlyReviveDeletedDocuments,
            0xd7 => Status::SubDocDeletedDocumentCantHaveValue,

            _ => Status::Unknown(value),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            Status::AuthError => "authentication error",
            Status::NotMyVbucket => "not my vbucket",
            Status::Success => "success",
            Status::TmpFail => "temporary failure",
            Status::AuthContinue => "authentication continue",
            Status::KeyExists => "key exists",
            Status::NotStored => "not stored",
            Status::TooBig => "too big",
            Status::Locked => "locked",
            Status::NotLocked => "not locked",
            Status::ScopeUnknown => "scope unknown",
            Status::CollectionUnknown => "collection unknown",
            Status::AccessError => "access error",
            Status::KeyNotFound => "key not found",
            Status::InvalidArgs => "invalid args",
            Status::NoBucket => "no bucket selected",
            Status::SubDocPathNotFound => "subdoc path not found",
            Status::SubDocPathMismatch => "subdoc path mismatch",
            Status::SubDocPathInvalid => "subdoc path invalid",
            Status::SubDocPathTooBig => "subdoc path too big",
            Status::SubDocDocTooDeep => "subdoc document too deep",
            Status::SubDocCantInsert => "subdoc can't insert",
            Status::SubDocNotJSON => "subdoc not JSON",
            Status::SubDocBadRange => "subdoc bad range",
            Status::SubDocBadDelta => "subdoc bad delta",
            Status::SubDocPathExists => "subdoc path exists",
            Status::SubDocValueTooDeep => "subdoc value too deep",
            Status::SubDocInvalidCombo => "subdoc invalid combo",
            Status::SubDocMultiPathFailure => "subdoc multipath failure",
            Status::SubDocSuccessDeleted => "subdoc success deleted",
            Status::SubDocXattrInvalidFlagCombo => "subdoc xattr invalid flag combo",
            Status::SubDocXattrInvalidKeyCombo => "subdoc xattr invalid key combo",
            Status::SubDocXattrUnknownMacro => "subdoc xattr unknown macro",
            Status::SubDocXattrUnknownVAttr => "subdoc xattr unknown vattr",
            Status::SubDocXattrCannotModifyVAttr => "subdoc xattr cannot modify vattr",
            Status::SubDocMultiPathFailureDeleted => "subdoc multipath failure deleted",
            Status::SubDocInvalidXattrOrder => "subdoc invalid xattr order",
            Status::SubDocXattrUnknownVattrMacro => "subdoc xattr unknown vattr macro",
            Status::SubDocCanOnlyReviveDeletedDocuments => {
                "subdoc can only revive deleted documents"
            }
            Status::SubDocDeletedDocumentCantHaveValue => {
                "subdoc deleted document can't have value"
            }
            Status::BadDelta => "bad delta",
            Status::OpaqueNoMatch => "opaque no match",
            Status::WouldThrottle => "would throttle",
            Status::ConfigOnly => "config only",
            Status::AuthStale => "authentication stale",
            Status::RangeError => "range error",
            Status::NotInitialized => "not initialized",
            Status::RateLimitedNetworkIngress => "rate limited: network ingress",
            Status::RateLimitedNetworkEgress => "rate limited: network egress",
            Status::RateLimitedMaxConnections => "rate limited: max connections",
            Status::RateLimitedMaxCommands => "rate limited: max commands",
            Status::RateLimitedScopeSizeLimitExceeded => "rate limited: scope size limit exceeded",
            Status::CommandUnknown => "unknown command",
            Status::OutOfMemory => "out of memory",
            Status::NotSupported => "not supported",
            Status::InternalError => "internal error",
            Status::Busy => "busy",
            Status::DurabilityInvalidLevel => "durability invalid level",
            Status::DurabilityImpossible => "durability impossible",
            Status::SyncWriteInProgress => "sync write in progress",
            Status::SyncWriteAmbiguous => "sync write ambiguous",
            Status::SyncWriteRecommitInProgress => "sync write recommit in progress",
            Status::RangeScanCancelled => "range scan cancelled",
            Status::RangeScanMore => "range scan more",
            Status::RangeScanComplete => "range scan complete",
            Status::RangeScanVBUUIDNotEqual => "range scan vb-uuid not equal",
            Status::Unknown(status) => {
                let t = format!("unknown status 0x{:x}", status);

                write!(f, "{}", t)?;
                return Ok(());
            }
        };

        write!(f, "{}", txt)
    }
}
