#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum BucketFeature {
    CreateAsDeleted,
    ReplaceBodyWithXattr,
    RangeScan,
    ReplicaRead,
    NonDedupedHistory,
    ReviveDocument,
}
