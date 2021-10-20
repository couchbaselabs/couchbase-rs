mod test_functions;
mod tests;
pub mod util;

use crate::util::mock::MockCluster;
use crate::util::standalone::StandaloneCluster;
use couchbase::CouchbaseError;
use env_logger::Env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::sync::Arc;
use util::*;

async fn setup() -> (ClusterUnderTest, Arc<TestConfig>) {
    let loaded_config = Config::try_load_config();
    let server = match loaded_config {
        Some(c) => match c.cluster_type() {
            ClusterType::Standalone => ClusterUnderTest::Standalone(StandaloneCluster::start(
                c.standalone_config()
                    .expect("Standalone config required when standalone type used."),
                c.tests(),
            )),
            ClusterType::Mock => {
                ClusterUnderTest::Mocked(MockCluster::start(c.mock_config(), c.tests()).await)
            }
        },
        None => ClusterUnderTest::Mocked(MockCluster::start(None, vec![]).await),
    };
    let config = server.config();

    (server, config)
}

fn teardown() {}

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

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::from_env(Env::default().default_filter_or("debug")).init();

    let config = setup().await;

    let mut success = 0;
    let mut failures = vec![];
    let mut skipped = 0;
    for t in test_functions::tests(config.1.clone()) {
        if config.1.clone().test_enabled(t.name.clone()) {
            println!();
            println!("Running {}", t.name.clone());
            let handle = tokio::spawn(t.func);
            let result = match handle.await {
                Ok(r) => match r {
                    Ok(was_skipped) => {
                        if was_skipped {
                            skipped += 1;
                            TestOutcome {
                                name: t.name.to_string(),
                                result: TestResultStatus::Skipped,
                                error: None,
                            }
                        } else {
                            success += 1;
                            TestOutcome {
                                name: t.name.to_string(),
                                result: TestResultStatus::Success,
                                error: None,
                            }
                        }
                    }
                    Err(e) => {
                        failures.push(t.name.clone());
                        TestOutcome {
                            name: t.name.to_string(),
                            result: TestResultStatus::Failure,
                            error: Some(e),
                        }
                    }
                },
                Err(_e) => {
                    // The JoinError here doesn't tell us anything interesting but the panic will be
                    // output to stderr anyway.
                    failures.push(t.name.clone());
                    TestOutcome {
                        name: t.name.to_string(),
                        result: TestResultStatus::Failure,
                        error: None,
                    }
                }
            };

            println!("{}", result);
            println!();
        } else {
            println!("Skipping {}, not enabled", t.name.clone());
            skipped += 1;
        }
    }

    teardown();

    println!();
    println!(
        "Success: {}, Failures: {}, Skipped: {}",
        success,
        failures.len(),
        skipped
    );
    if failures.len() > 0 {
        println!("Failed: {}", failures.join(", "));
    }
    println!();

    if failures.len() == 0 {
        Ok(())
    } else {
        Err(std::io::Error::new(ErrorKind::Other, "test failures"))
    }
}
