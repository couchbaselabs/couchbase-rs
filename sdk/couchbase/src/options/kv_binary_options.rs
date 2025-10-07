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

use crate::durability_level::DurabilityLevel;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AppendOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
}

impl AppendOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct PrependOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
}

impl PrependOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct IncrementOptions {
    pub expiry: Option<Duration>,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
}

impl IncrementOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn initial(mut self, initial: u64) -> Self {
        self.initial = Some(initial);
        self
    }

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DecrementOptions {
    pub expiry: Option<Duration>,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
}

impl DecrementOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn initial(mut self, initial: u64) -> Self {
        self.initial = Some(initial);
        self
    }

    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }
}
