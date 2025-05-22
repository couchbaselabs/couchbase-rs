use crate::durability_level::DurabilityLevel;
use std::time::Duration;

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AppendOptions {
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) cas: Option<u64>,
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
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) cas: Option<u64>,
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
    pub(crate) expiry: Option<Duration>,
    pub(crate) initial: Option<u64>,
    pub(crate) delta: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
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
    pub(crate) expiry: Option<Duration>,
    pub(crate) initial: Option<u64>,
    pub(crate) delta: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
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
