use strum_macros::EnumIter;

#[derive(Debug, Clone, Copy, Eq, PartialEq, EnumIter)]
pub enum TestFeature {
    KeyValue,
    Query,
    Subdoc,
    Xattrs,
    ExpandMacros,
    PreserveExpiry,
    Replicas,
    Durability,
    CollectionsQuery,
}
