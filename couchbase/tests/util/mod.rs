mod config;
mod features;
mod mock;
mod standalone;

use mock::MockCluster;
use standalone::StandaloneCluster;

pub use crate::util::features::TestFeature;
use config::{ClusterType, Config};
use couchbase::{Bucket, Cluster, Collection, Scope};
use env_logger::Env;
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

    env_logger::from_env(Env::default().default_filter_or("warn")).init();

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
    support_matrix: Vec<TestFeature>,
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
    pub fn supports_feature(&self, feature: TestFeature) -> bool {
        self.support_matrix.contains(&feature)
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
