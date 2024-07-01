use std::time::Duration;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DurabilityLevel {
    Majority,
    MajorityAndPersistToActive,
    PersistToMajority,
    Unknown,
}

impl From<DurabilityLevel> for u8 {
    fn from(value: DurabilityLevel) -> u8 {
        match value {
            DurabilityLevel::Majority => 1,
            DurabilityLevel::MajorityAndPersistToActive => 2,
            DurabilityLevel::PersistToMajority => 3,
            DurabilityLevel::Unknown => 0,
        }
    }
}

impl From<u8> for DurabilityLevel {
    fn from(data: u8) -> Self {
        match data {
            1 => DurabilityLevel::Majority,
            2 => DurabilityLevel::MajorityAndPersistToActive,
            3 => DurabilityLevel::PersistToMajority,
            _ => DurabilityLevel::Unknown,
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
