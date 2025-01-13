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

impl TryFrom<&str> for DurabilityLevel {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "majority" => Ok(DurabilityLevel::Majority),
            "majorityAndPersistToActive" => Ok(DurabilityLevel::MajorityAndPersistToActive),
            "persistToMajority" => Ok(DurabilityLevel::PersistToMajority),
            _ => Err(Self::Error {
                msg: "unknown durability level".to_string(),
            }),
        }
    }
}

impl TryFrom<String> for DurabilityLevel {
    type Error = crate::error::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}
