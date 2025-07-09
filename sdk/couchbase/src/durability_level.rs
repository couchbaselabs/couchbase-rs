use std::fmt::Display;

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct DurabilityLevel(InnerDurabilityLevel);

impl DurabilityLevel {
    pub const NONE: DurabilityLevel = DurabilityLevel(InnerDurabilityLevel::None);

    pub const MAJORITY: DurabilityLevel = DurabilityLevel(InnerDurabilityLevel::Majority);

    pub const MAJORITY_AND_PERSIST_ACTIVE: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::MajorityAndPersistActive);

    pub const PERSIST_TO_MAJORITY: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::PersistToMajority);

    pub(crate) fn other(val: String) -> DurabilityLevel {
        DurabilityLevel(InnerDurabilityLevel::Other(val))
    }
}

impl Display for DurabilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerDurabilityLevel::None => write!(f, "none"),
            InnerDurabilityLevel::Majority => write!(f, "majority"),
            InnerDurabilityLevel::MajorityAndPersistActive => write!(f, "majorityAndPersistActive"),
            InnerDurabilityLevel::PersistToMajority => write!(f, "persistToMajority"),
            InnerDurabilityLevel::Other(val) => write!(f, "unknown({val})"),
        }
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerDurabilityLevel {
    None,
    Majority,
    MajorityAndPersistActive,
    PersistToMajority,
    Other(String),
}

pub(crate) fn parse_optional_durability_level_to_memdx(
    durability_level: Option<DurabilityLevel>,
) -> Option<couchbase_core::memdx::durability_level::DurabilityLevel> {
    match durability_level {
        Some(DurabilityLevel(InnerDurabilityLevel::Majority)) => {
            Some(couchbase_core::memdx::durability_level::DurabilityLevel::MAJORITY)
        }
        Some(DurabilityLevel(InnerDurabilityLevel::MajorityAndPersistActive)) => Some(
            couchbase_core::memdx::durability_level::DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE,
        ),
        Some(DurabilityLevel(InnerDurabilityLevel::PersistToMajority)) => {
            Some(couchbase_core::memdx::durability_level::DurabilityLevel::PERSIST_TO_MAJORITY)
        }
        _ => None,
    }
}

impl From<DurabilityLevel> for couchbase_core::mgmtx::bucket_settings::DurabilityLevel {
    fn from(value: DurabilityLevel) -> Self {
        match value {
            DurabilityLevel::NONE => couchbase_core::mgmtx::bucket_settings::DurabilityLevel::NONE,
            DurabilityLevel::MAJORITY => {
                couchbase_core::mgmtx::bucket_settings::DurabilityLevel::MAJORITY
            }
            DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE => {
                couchbase_core::mgmtx::bucket_settings::DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE
            }
            DurabilityLevel::PERSIST_TO_MAJORITY => {
                couchbase_core::mgmtx::bucket_settings::DurabilityLevel::PERSIST_TO_MAJORITY
            }
            _ => unreachable!(),
        }
    }
}

impl From<couchbase_core::mgmtx::bucket_settings::DurabilityLevel> for DurabilityLevel {
    fn from(value: couchbase_core::mgmtx::bucket_settings::DurabilityLevel) -> Self {
        match value {
            couchbase_core::mgmtx::bucket_settings::DurabilityLevel::NONE => DurabilityLevel::NONE,
            couchbase_core::mgmtx::bucket_settings::DurabilityLevel::MAJORITY => DurabilityLevel::MAJORITY,
            couchbase_core::mgmtx::bucket_settings::DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE => DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE,
            couchbase_core::mgmtx::bucket_settings::DurabilityLevel::PERSIST_TO_MAJORITY => DurabilityLevel::PERSIST_TO_MAJORITY,
            _ => DurabilityLevel::other(value.to_string()),
        }
    }
}
