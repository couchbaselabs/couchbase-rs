use crate::memdx::error;
use crate::memdx::opcode::OpCode;
use bitflags::bitflags;

pub trait SubdocOp {
    fn is_xattr_op(&self) -> bool;
}

pub fn reorder_subdoc_ops<T: SubdocOp>(ops: &[T]) -> (Vec<&T>, Vec<usize>) {
    let mut ordered_ops = Vec::with_capacity(ops.len());
    let mut op_indexes = Vec::with_capacity(ops.len());

    for (i, op) in ops.iter().enumerate() {
        if op.is_xattr_op() {
            ordered_ops.push(op);
            op_indexes.push(i);
        }
    }

    for (i, op) in ops.iter().enumerate() {
        if !op.is_xattr_op() {
            ordered_ops.push(op);
            op_indexes.push(i);
        }
    }

    (ordered_ops, op_indexes)
}

// Request-info needed for response parsing
#[derive(Debug, Clone, Copy, Default)]
pub struct SubdocRequestInfo {
    pub(crate) flags: SubdocDocFlag,
    pub(crate) op_count: u8,
}

#[derive(Debug)]
pub struct SubDocResult {
    pub err: Option<error::Error>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub struct LookupInOp<'a> {
    pub(crate) op: LookupInOpType,
    pub(crate) flags: SubdocOpFlag,
    pub(crate) path: &'a [u8],
}

impl<'a> LookupInOp<'a> {
    pub fn new(op: LookupInOpType, path: &'a [u8]) -> Self {
        Self {
            op,
            flags: SubdocOpFlag::empty(),
            path,
        }
    }

    pub fn flags(mut self, flags: SubdocOpFlag) -> Self {
        self.flags = flags;
        self
    }
}

impl SubdocOp for LookupInOp<'_> {
    fn is_xattr_op(&self) -> bool {
        self.flags.contains(SubdocOpFlag::XATTR_PATH)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutateInOp<'a> {
    pub(crate) op: MutateInOpType,
    pub(crate) flags: SubdocOpFlag,
    pub(crate) path: &'a [u8],
    pub(crate) value: &'a [u8],
}

impl<'a> MutateInOp<'a> {
    pub fn new(op: MutateInOpType, path: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            op,
            flags: SubdocOpFlag::empty(),
            path,
            value,
        }
    }

    pub fn flags(mut self, flags: SubdocOpFlag) -> Self {
        self.flags = flags;
        self
    }
}

impl SubdocOp for MutateInOp<'_> {
    fn is_xattr_op(&self) -> bool {
        self.flags.contains(SubdocOpFlag::XATTR_PATH)
    }
}

bitflags! {
    #[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
    pub struct SubdocOpFlag: u8 {
        // SubdocOpFlagMkDirP indicates that the path should be created if it does not already exist.
        const MKDIR_P = 0x01;

        // 0x02 is unused, formally SubdocFlagMkDoc

        // SubdocOpFlagXattrPath indicates that the path refers to an Xattr rather than the document body.
        const XATTR_PATH = 0x04;

        // 0x08 is unused, formally SubdocFlagAccessDeleted

        // SubdocOpFlagExpandMacros indicates that the value portion of any sub-document mutations
        // should be expanded if they contain macros such as \${Mutation.CAS}.
        const EXPAND_MACROS = 0x10;
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Default, Ord, PartialOrd, Hash)]
    pub struct SubdocDocFlag: u8 {
        // SubdocDocFlagMkDoc indicates that the document should be created if it does not already exist.
        const MkDoc = 0x01;
        // SubdocDocFlagAddDoc indices that this operation should be an add rather than set.
        const AddDoc = 0x02;

        // SubdocDocFlagAccessDeleted indicates that you wish to receive soft-deleted documents.
        // Internal: This should never be used and is not supported.
        const AccessDeleted = 0x04;

        // SubdocDocFlagCreateAsDeleted indicates that the document should be created as deleted.
        // That is, to create a tombstone only.
        // Internal: This should never be used and is not supported.
        const CreateAsDeleted = 0x08;

        // SubdocDocFlagReviveDocument indicates that the document should be revived from a tombstone.
        // Internal: This should never be used and is not supported.
        const ReviveDocument = 0x10;
    }
}

// LookupInOpType specifies the type of lookup in operation.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum LookupInOpType {
    // LookupInOpTypeGet indicates the operation is a sub-document `Get` operation.
    Get,
    // LookupInOpTypeExists indicates the operation is a sub-document `Exists` operation.
    Exists,
    // LookupInOpTypeGetCount indicates the operation is a sub-document `GetCount` operation.
    GetCount,
    // LookupInOpTypeGetDoc represents a full document retrieval, for use with extended attribute ops.
    GetDoc,
}

impl From<LookupInOpType> for OpCode {
    fn from(op_type: LookupInOpType) -> Self {
        match op_type {
            LookupInOpType::Get => OpCode::SubDocGet,
            LookupInOpType::Exists => OpCode::SubDocExists,
            LookupInOpType::GetCount => OpCode::SubDocGetCount,
            LookupInOpType::GetDoc => OpCode::Get,
        }
    }
}

// MutateInOpType specifies the type of mutate in operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MutateInOpType {
    // MutateInOpTypeDictAdd indicates the operation is a sub-document `Add` operation.
    DictAdd,
    // MutateInOpTypeDictSet indicates the operation is a sub-document `Set` operation.
    DictSet,
    // MutateInOpTypeDelete indicates the operation is a sub-document `Remove` operation.
    Delete,
    // MutateInOpTypeReplace indicates the operation is a sub-document `Replace` operation.
    Replace,
    // MutateInOpTypeArrayPushLast indicates the operation is a sub-document `ArrayPushLast` operation.
    ArrayPushLast,
    // MutateInOpTypeArrayPushFirst indicates the operation is a sub-document `ArrayPushFirst` operation.
    ArrayPushFirst,
    // MutateInOpTypeArrayInsert indicates the operation is a sub-document `ArrayInsert` operation.
    ArrayInsert,
    // MutateInOpTypeArrayAddUnique indicates the operation is a sub-document `ArrayAddUnique` operation.
    ArrayAddUnique,
    // MutateInOpTypeCounter indicates the operation is a sub-document `Counter` operation.
    Counter,
    // MutateInOpTypeSetDoc represents a full document set, for use with extended attribute ops.
    SetDoc,
    // MutateInOpTypeAddDoc represents a full document add, for use with extended attribute ops.
    AddDoc,
    // MutateInOpTypeDeleteDoc represents a full document delete, for use with extended attribute ops.
    DeleteDoc,
    // MutateInOpTypeReplaceBodyWithXattr represents a replace body with xattr op.
    // Uncommitted: This API may change in the future.
    ReplaceBodyWithXattr,
}

impl From<MutateInOpType> for OpCode {
    fn from(op_type: MutateInOpType) -> Self {
        match op_type {
            MutateInOpType::DictAdd => OpCode::SubDocDictAdd,
            MutateInOpType::DictSet => OpCode::SubDocDictSet,
            MutateInOpType::Delete => OpCode::SubDocDelete,
            MutateInOpType::Replace => OpCode::SubDocReplace,
            MutateInOpType::ArrayPushLast => OpCode::SubDocArrayPushLast,
            MutateInOpType::ArrayPushFirst => OpCode::SubDocArrayPushFirst,
            MutateInOpType::ArrayInsert => OpCode::SubDocArrayInsert,
            MutateInOpType::ArrayAddUnique => OpCode::SubDocArrayAddUnique,
            MutateInOpType::Counter => OpCode::SubDocCounter,
            MutateInOpType::SetDoc => OpCode::Set,
            MutateInOpType::AddDoc => OpCode::Add,
            MutateInOpType::DeleteDoc => OpCode::Delete,
            MutateInOpType::ReplaceBodyWithXattr => OpCode::SubDocReplaceBodyWithXattr,
        }
    }
}

pub const SUBDOC_XATTR_PATH_HLC: &[u8] = b"$vbucket.HLC";

pub const SUBDOC_MACRO_NEW_CRC32C: &[u8] = b"\"${Mutation.value_crc32c}\"";
pub const SUBDOC_MACRO_NEW_CAS: &[u8] = b"\"${Mutation.CAS}\"";
pub const SUBDOC_MACRO_OLD_REVID: &[u8] = b"\"${$document.revid}\"";
pub const SUBDOC_MACRO_OLD_EXPTIME: &[u8] = b"\"${$document.exptime}\"";
pub const SUBDOC_MACRO_OLD_CAS: &[u8] = b"\"${$document.CAS}\"";
