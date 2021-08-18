use super::{ConfigAware, TestConfig};
use crate::util::config::StandaloneConfig;
use couchbase::Cluster;
use std::sync::Arc;

pub(crate) struct StandaloneCluster {
    config: Arc<TestConfig>,
}

impl StandaloneCluster {
    pub fn start(c: StandaloneConfig) -> Self {
        let cluster = Cluster::connect(c.conn_string(), c.username(), c.password());
        let bucket = cluster.bucket(c.default_bucket().unwrap_or_else(|| "default".into()));
        let scope = bucket.scope(c.default_scope().unwrap_or_else(|| "_default".into()));
        let collection =
            bucket.collection(c.default_collection().unwrap_or_else(|| "_default".into()));

        Self {
            config: Arc::new(TestConfig {
                cluster,
                bucket,
                scope,
                collection,
            }),
        }
    }
}

impl ConfigAware for StandaloneCluster {
    fn config(&self) -> Arc<TestConfig> {
        self.config.clone()
    }
}

impl Drop for StandaloneCluster {
    fn drop(&mut self) {
        todo!();
    }
}
