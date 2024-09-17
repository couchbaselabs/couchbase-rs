use crate::mutationtoken::MutationToken;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub value: Vec<u8>,
    pub flags: u32,
    pub datatype: u8,
    pub cas: u64,
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
pub struct UnlockResult {
}

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
