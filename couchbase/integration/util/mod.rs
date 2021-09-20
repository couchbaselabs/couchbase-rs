pub mod config;
pub mod features;
pub mod mock;
pub mod standalone;

use mock::MockCluster;
use standalone::StandaloneCluster;

pub use crate::util::features::TestFeature;
pub use config::{ClusterType, Config};
use couchbase::{Bucket, Cluster, Collection, Scope};
use std::sync::Arc;

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

pub enum ClusterUnderTest {
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

pub trait ConfigAware {
    fn config(&self) -> Arc<TestConfig>;
}

// pub fn block_on<F: Future>(future: F) -> F::Output {
//     let rt = Runtime::new().unwrap();
//     rt.spawn(future).
// }
