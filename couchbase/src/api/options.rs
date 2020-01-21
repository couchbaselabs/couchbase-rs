use std::time::Duration;

#[derive(Debug)]
pub struct QueryOptions {
    timeout: Option<Duration>,
    pub(crate) scan_consistency: QueryScanConsistency,
}

impl QueryOptions {

    pub fn scan_consistency(mut self, scan_consistency: QueryScanConsistency) -> Self {
        self.scan_consistency = scan_consistency;
        self
    }

}

#[derive(Debug)]
pub enum QueryScanConsistency {
    NotBounded,
    RequestPlus,
}

impl QueryOptions {
    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self { timeout: None, scan_consistency: QueryScanConsistency::NotBounded }
    }
}

#[derive(Debug)]
pub struct GetOptions {
    timeout: Option<Duration>,
}

impl GetOptions {
    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        Self { timeout: None }
    }
}

#[derive(Debug)]
pub struct UpsertOptions {
    timeout: Option<Duration>,
}

impl UpsertOptions {
    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}

impl Default for UpsertOptions {
    fn default() -> Self {
        Self { timeout: None }
    }
}