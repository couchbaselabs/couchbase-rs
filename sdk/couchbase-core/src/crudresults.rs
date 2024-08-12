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
