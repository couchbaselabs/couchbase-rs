mod mock;
mod standalone;

use mock::MockCluster;
use standalone::StandaloneCluster;
use std::ffi::OsStr;
use std::panic;

use lazy_static::lazy_static;
use std::env;
use std::sync::Mutex;

lazy_static! {
    static ref CLUSTER: Mutex<Option<ClusterUnderTest>> = Mutex::new(None);
}

fn setup() -> TestConfig {
    // todo: load config from a .toml file in the tests dir
    // implement setup and teardown for standalone
    // implement setup and teardown for mocked
    // send config into callbacks

    let server = match env::var_os("TEST_CLUSTER_TYPE") {
        Some(s) if s == OsStr::new("standalone") => {
            ClusterUnderTest::Standalone(StandaloneCluster::start())
        }
        _ => ClusterUnderTest::Mocked(MockCluster::start()),
    };
    let config = server.config();

    *CLUSTER.lock().unwrap() = Some(server);
    config
}

fn teardown() {
    let _ = CLUSTER.lock().unwrap().take();
}

pub fn run<T>(test: T)
where
    T: FnOnce(TestConfig) -> () + panic::UnwindSafe,
{
    let config = setup();
    let _ = panic::catch_unwind(|| test(config));
    teardown();
}

#[derive(Debug)]
pub struct TestConfig {}

enum ClusterUnderTest {
    Standalone(StandaloneCluster),
    Mocked(MockCluster),
}

impl ConfigAware for ClusterUnderTest {
    fn config(&self) -> TestConfig {
        match self {
            ClusterUnderTest::Standalone(s) => s.config(),
            ClusterUnderTest::Mocked(m) => m.config(),
        }
    }
}

trait ConfigAware {
    fn config(&self) -> TestConfig;
}
