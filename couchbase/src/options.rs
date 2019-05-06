//! Option arguments for all operations.

use std::time::Duration;

#[derive(Debug, Default)]
pub struct GetOptions {
    timeout: Option<Duration>,
}

impl GetOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct GetAndLockOptions {
    timeout: Option<Duration>,
    lock_for: Option<Duration>,
}

impl GetAndLockOptions {
    pub fn new() -> Self {
        Self {
            timeout: None,
            lock_for: None,
        }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }

    pub fn set_lock_for(mut self, lock_for: Duration) -> Self {
        self.lock_for = Some(lock_for);
        self
    }

    pub fn lock_for(&self) -> &Option<Duration> {
        &self.lock_for
    }
}

#[derive(Debug, Default)]
pub struct GetAndTouchOptions {
    timeout: Option<Duration>,
}

impl GetAndTouchOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct InsertOptions {
    timeout: Option<Duration>,
}

impl InsertOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct UpsertOptions {
    timeout: Option<Duration>,
}

impl UpsertOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct ReplaceOptions {
    timeout: Option<Duration>,
    cas: Option<u64>,
}

impl ReplaceOptions {
    pub fn new() -> Self {
        Self {
            timeout: None,
            cas: None,
        }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }

    pub fn set_cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    pub fn cas(&self) -> &Option<u64> {
        &self.cas
    }
}

#[derive(Debug, Default)]
pub struct RemoveOptions {
    timeout: Option<Duration>,
}

impl RemoveOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct QueryOptions {
    timeout: Option<Duration>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct AnalyticsOptions {
    timeout: Option<Duration>,
}

impl AnalyticsOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct TouchOptions {
    timeout: Option<Duration>,
}

impl TouchOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct UnlockOptions {
    timeout: Option<Duration>,
}

impl UnlockOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct ExistsOptions {
    timeout: Option<Duration>,
}

impl ExistsOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

#[derive(Debug, Default)]
pub struct LookupInOptions {
    timeout: Option<Duration>,
}

impl LookupInOptions {
    pub fn new() -> Self {
        Self { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}
