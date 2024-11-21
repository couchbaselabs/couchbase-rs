use crate::mutationtoken::MutationToken;
use std::time::Duration;
use crate::memdx::subdoc::SubDocResult;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub value: Vec<u8>,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetMetaResult {
    pub cas: u64,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
    pub expiry: u32,
    pub seq_no: u64,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpsertResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeleteResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetAndLockResult {
    pub value: Vec<u8>,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetAndTouchResult {
    pub value: Vec<u8>,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UnlockResult {}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TouchResult {
    pub cas: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AddResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplaceResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AppendResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PrependResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IncrementResult {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DecrementResult {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
}

#[derive(Clone, Debug)]
pub struct LookupInResult {
    pub value: Vec<SubDocResult>,
    pub cas: u64,
    pub doc_is_deleted: bool
}

#[derive(Clone, Debug)]
pub struct MutateInResult {
    pub value: Vec<SubDocResult>,
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}
