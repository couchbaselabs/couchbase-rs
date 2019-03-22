use std::time::Duration;

#[derive(Debug)]
pub struct GetOptions {
    timeout: Option<Duration>,
}

impl GetOptions {
    pub fn new() -> Self {
        GetOptions { timeout: None }
    }

    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn timeout(&self) -> &Option<Duration> {
        &self.timeout
    }
}
