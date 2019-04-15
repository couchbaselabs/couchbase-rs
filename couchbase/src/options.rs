use std::time::Duration;

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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
