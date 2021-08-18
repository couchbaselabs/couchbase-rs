mod config;
mod mock;
mod standalone;

use mock::MockCluster;
use standalone::StandaloneCluster;

use config::{ClusterType, Config};
use couchbase::{Bucket, Cluster, Collection, Scope};
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

lazy_static! {
    static ref CLUSTER: Mutex<Option<ClusterUnderTest>> = Mutex::new(None);
}

pub async fn setup() -> Arc<TestConfig> {
    // todo: implement setup and teardown for standalone
    // implement setup and teardown for mocked
    // send config into callbacks

    let mut guard = CLUSTER.lock().unwrap();
    if (*guard).is_some() {
        return (*guard).as_ref().unwrap().config();
    }

    env_logger::init();

    let loaded_config = Config::try_load_config();
    let server = match loaded_config {
        Some(c) => match c.cluster_type() {
            ClusterType::Standalone => ClusterUnderTest::Standalone(StandaloneCluster::start(
                c.standalone_config()
                    .expect("Standalone config required when standalone type used."),
            )),
            ClusterType::Mock => {
                ClusterUnderTest::Mocked(MockCluster::start(c.mock_config()).await)
            }
        },
        None => ClusterUnderTest::Mocked(MockCluster::start(None).await),
    };
    let config = server.config();

    *guard = Some(server);

    config
}

// fn teardown() {
//     let _ = CLUSTER.lock().unwrap().take();
// }

// pub fn run<T>(test: T)
// where
//     T: FnOnce(Arc<TestConfig>) -> () + panic::UnwindSafe,
// {
//     let config = setup();
//     let _ = panic::catch_unwind(|| test(config));
//     teardown();
// }

#[derive(Debug)]
pub struct TestConfig {
    cluster: Cluster,
    bucket: Bucket,
    scope: Scope,
    collection: Collection,
}

impl TestConfig {
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }
    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn collection(&self) -> &Collection {
        &self.collection
    }
}

enum ClusterUnderTest {
    Standalone(StandaloneCluster),
    Mocked(MockCluster),
}

impl ConfigAware for ClusterUnderTest {
    fn config(&self) -> Arc<TestConfig> {
        match self {
            ClusterUnderTest::Standalone(s) => s.config(),
            ClusterUnderTest::Mocked(m) => m.config(),
        }
    }
}

trait ConfigAware {
    fn config(&self) -> Arc<TestConfig>;
}
