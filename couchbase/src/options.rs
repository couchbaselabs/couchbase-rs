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
}

impl ReplaceOptions {
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
