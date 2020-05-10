use super::{ConfigAware, TestConfig};

pub struct MockCluster {}

impl MockCluster {
    pub fn start() -> Self {
        todo!();
    }
}

impl ConfigAware for MockCluster {
    fn config(&self) -> TestConfig {
        todo!();
    }
}

impl Drop for MockCluster {
    fn drop(&mut self) {
        todo!();
    }
}
