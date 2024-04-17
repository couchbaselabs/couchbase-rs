use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use crate::CouchbaseError;
use crate::test::{Config, TestConfig};
use crate::test::mock::MockCluster;
use crate::test::ConfigAware;
async fn setup() -> Arc<TestConfig> {
    let loaded_config = Config::try_load_config();
    let server = MockCluster::start(None, vec![]).await;
    let config = server.config();

    config
}


#[derive(Debug, Copy, Clone)]
enum TestResultStatus {
    Success,
    Failure,
    Skipped,
}

impl Display for TestResultStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let alias = match *self {
            TestResultStatus::Success => "success",
            TestResultStatus::Failure => "failure",
            TestResultStatus::Skipped => "skipped",
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug)]
pub struct TestError {
    reason: String,
}

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason.clone())
    }
}

impl Error for TestError {}

impl From<CouchbaseError> for TestError {
    fn from(e: CouchbaseError) -> Self {
        Self {
            reason: e.to_string(),
        }
    }
}

pub type TestResult<T, E = TestError> = std::result::Result<T, E>;

#[derive(Debug)]
struct TestOutcome {
    name: String,
    result: TestResultStatus,
    error: Option<TestError>,
}

impl Display for TestOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut out = format!("{} -> {}", self.name.clone(), self.result);
        if let Some(e) = &self.error {
            out = format!("{}: {}", out, e);
        }
        write!(f, "{}", out)
    }
}
