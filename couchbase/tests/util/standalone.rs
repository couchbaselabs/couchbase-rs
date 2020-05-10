use super::{ConfigAware, TestConfig};

pub struct StandaloneCluster {}

impl StandaloneCluster {
    pub fn start() -> Self {
        todo!();
    }
}

impl ConfigAware for StandaloneCluster {
    fn config(&self) -> TestConfig {
        todo!();
    }
}

impl Drop for StandaloneCluster {
    fn drop(&mut self) {
        todo!();
    }
}
