use crate::memdx::response::GetResponse;
use crate::mutationtoken::MutationToken;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub value: Vec<u8>,
    pub flags: u32,
    // datatype: Dataty
    pub cas: u64,
}

impl From<GetResponse> for GetResult {
    fn from(resp: GetResponse) -> Self {
        Self {
            value: resp.value,
            flags: resp.flags,
            cas: resp.cas,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpsertResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
}
