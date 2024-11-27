use std::fmt::{Display, Formatter, LowerHex};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Status {
    AuthError,
    NotMyVbucket,
    Success,
    TmpFail,
    SASLAuthContinue,
    KeyExists,
    NotStored,
    Locked,
    NotLocked,
    TooBig,
    ScopeUnknown,
    CollectionUnknown,
    AccessError,
    KeyNotFound,
    InvalidArgs,
    NoBucket,
    SubDocPathNotFound,
    SubDocPathMismatch,
    SubDocPathInvalid,
    SubDocPathTooBig,
    SubDocDocTooDeep,
    SubDocCantInsert,
    SubDocNotJSON,
    SubDocBadRange,
    SubDocBadDelta,
    SubDocPathExists,
    SubDocValueTooDeep,
    SubDocInvalidCombo,
    SubDocMultiPathFailure,
    SubDocSuccessDeleted,
    SubDocXattrInvalidFlagCombo,
    SubDocXattrInvalidKeyCombo,
    SubDocXattrUnknownMacro,
    SubDocXattrUnknownVAttr,
    SubDocXattrCannotModifyVAttr,
    SubDocMultiPathFailureDeleted,
    SubDocInvalidXattrOrder,
    SubDocXattrUnknownVattrMacro,
    SubDocCanOnlyReviveDeletedDocuments,
    SubDocDeletedDocumentCantHaveValue,

    Unknown(u16),
}

impl From<Status> for u16 {
    fn from(value: Status) -> Self {
        match value {
            Status::Success => 0x00,
            Status::KeyNotFound => 0x01,
            Status::KeyExists => 0x02,
            Status::TooBig => 0x03,
            Status::InvalidArgs => 0x04,
            Status::NotStored => 0x05,
            Status::NotMyVbucket => 0x07,
            Status::NoBucket => 0x08,
            Status::Locked => 0x09,
            Status::NotLocked => 0x0e,
            Status::AuthError => 0x20,
            Status::SASLAuthContinue => 0x21,
            Status::AccessError => 0x24,
            Status::TmpFail => 0x86,
            Status::ScopeUnknown => 0x8c,
            Status::CollectionUnknown => 0x88,
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

            Status::Unknown(value) => value,
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
            0x07 => Status::NotMyVbucket,
            0x08 => Status::NoBucket,
            0x09 => Status::Locked,
            0x0e => Status::NotLocked,
            0x20 => Status::AuthError,
            0x21 => Status::SASLAuthContinue,
            0x24 => Status::AccessError,
            0x86 => Status::TmpFail,
            0x8c => Status::ScopeUnknown,
            0x88 => Status::CollectionUnknown,
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
            Status::SASLAuthContinue => "authentication continue",
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
            Status::Unknown(status) => {
                // TODO: improve this.
                let t = format!("unknown status {}", status);

                write!(f, "{}", t)?;
                return Ok(());
            }
        };

        write!(f, "{}", txt)
    }
}
