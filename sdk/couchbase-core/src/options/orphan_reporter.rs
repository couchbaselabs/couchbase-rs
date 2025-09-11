use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
#[non_exhaustive]
pub struct OrphanReporterConfig {
    pub reporter_interval: Duration,
    pub sample_size: usize,
    pub log_sink: Option<Arc<OrphanSinkFn>>,
}

// Type to capture orphan reporter output, primarily used for testing currently
pub type OrphanSinkFn = dyn Fn(&str) + Send + Sync + 'static;

impl OrphanReporterConfig {
    pub fn reporter_interval(mut self, reporter_interval: Duration) -> Self {
        self.reporter_interval = reporter_interval;
        self
    }

    pub fn sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }

    pub fn log_sink(mut self, log_sink: Arc<OrphanSinkFn>) -> Self {
        self.log_sink = Some(log_sink);
        self
    }
}

impl Default for OrphanReporterConfig {
    fn default() -> Self {
        Self {
            reporter_interval: Duration::from_secs(10),
            sample_size: 10,
            log_sink: None,
        }
    }
}
