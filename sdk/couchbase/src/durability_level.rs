use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum DurabilityLevel {
    Majority,
    MajorityAndPersistToActive,
    PersistToMajority,
}

impl From<DurabilityLevel> for couchbase_core::memdx::durability_level::DurabilityLevel {
    fn from(value: DurabilityLevel) -> Self {
        match value {
            DurabilityLevel::Majority => {
                couchbase_core::memdx::durability_level::DurabilityLevel::Majority
            }
            DurabilityLevel::MajorityAndPersistToActive => {
                couchbase_core::memdx::durability_level::DurabilityLevel::MajorityAndPersistToActive
            }
            DurabilityLevel::PersistToMajority => {
                couchbase_core::memdx::durability_level::DurabilityLevel::PersistToMajority
            }
        }
    }
}
