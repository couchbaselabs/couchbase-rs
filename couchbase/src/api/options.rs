use std::time::Duration;

#[derive(Debug)]
pub struct QueryOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) scan_consistency: Option<QueryScanConsistency>,
}

impl QueryOptions {
    pub fn scan_consistency(mut self, scan_consistency: QueryScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[derive(Debug)]
pub enum QueryScanConsistency {
    NotBounded,
    RequestPlus,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            scan_consistency: None,
        }
    }
}

#[derive(Debug)]
pub struct GetOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetOptions {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        Self { timeout: None }
    }
}

#[derive(Debug)]
pub struct UpsertOptions {
    pub(crate) timeout: Option<Duration>,
}

impl UpsertOptions {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl Default for UpsertOptions {
    fn default() -> Self {
        Self { timeout: None }
    }
}
