#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TestFeature {
    KeyValue,
    Query,
    Subdoc,
    Xattrs,
    ExpandMacros,
    PreserveExpiry,
    Replicas,
    Durability,
}
