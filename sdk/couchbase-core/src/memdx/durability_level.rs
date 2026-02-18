/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::tracingcomponent::IntoDurabilityU8;
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

impl IntoDurabilityU8 for DurabilityLevel {
    fn as_u8(&self) -> u8 {
        match self.0 {
            InnerDurabilityLevel::Majority => 1,
            InnerDurabilityLevel::MajorityAndPersistActive => 2,
            InnerDurabilityLevel::PersistToMajority => 3,
            InnerDurabilityLevel::Other(val) => val,
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
