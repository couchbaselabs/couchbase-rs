#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub vbid: u16,
    pub vbuuid: u64,
    pub seqno: u64,
}
