use std::fmt::Display;
use std::time::Duration;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct DurabilityLevel(InnerDurabilityLevel);

impl DurabilityLevel {
    pub const MAJORITY: DurabilityLevel = DurabilityLevel(InnerDurabilityLevel::Majority);

    pub const MAJORITY_AND_PERSIST_ACTIVE: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::MajorityAndPersistActive);

    pub const PERSIST_TO_MAJORITY: DurabilityLevel =
        DurabilityLevel(InnerDurabilityLevel::PersistToMajority);
}

impl Display for DurabilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            InnerDurabilityLevel::Majority => write!(f, "majority"),
            InnerDurabilityLevel::MajorityAndPersistActive => write!(f, "majorityAndPersistActive"),
            InnerDurabilityLevel::PersistToMajority => write!(f, "persistToMajority"),
            InnerDurabilityLevel::Other(val) => write!(f, "unknown({val})"),
        }
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) enum InnerDurabilityLevel {
    Majority,
    MajorityAndPersistActive,
    PersistToMajority,
    Other(u8),
}

impl From<DurabilityLevel> for u8 {
    fn from(value: DurabilityLevel) -> u8 {
        match value {
            DurabilityLevel::MAJORITY => 1,
            DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE => 2,
            DurabilityLevel::PERSIST_TO_MAJORITY => 3,
            _ => 0,
        }
    }
}

impl From<u8> for DurabilityLevel {
    fn from(data: u8) -> Self {
        match data {
            1 => DurabilityLevel::MAJORITY,
            2 => DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE,
            3 => DurabilityLevel::PERSIST_TO_MAJORITY,
            _ => DurabilityLevel(InnerDurabilityLevel::Other(data)),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct DurabilityLevelSettings {
    pub durability_level: DurabilityLevel,
    pub timeout: Option<Duration>,
}

impl DurabilityLevelSettings {
    pub fn new(level: DurabilityLevel) -> Self {
        Self {
            durability_level: level,
            timeout: None,
        }
    }

    pub fn new_with_timeout(level: DurabilityLevel, timeout: Duration) -> Self {
        Self {
            durability_level: level,
            timeout: Some(timeout),
        }
    }
}
