use crate::common::node_version::NodeVersion;
use crate::common::test_bucket::TestBucket;
use crate::common::test_config::TestSetupConfig;
use crate::common::test_manager::{TestBucketManager, TestUserManager};
use couchbase::cluster::Cluster;
use couchbase::error;
use couchbase::options::query_options::QueryOptions;
use couchbase::options::search_options::SearchOptions;
use couchbase::results::query_results::QueryResult;
use couchbase::results::search_results::SearchResult;
use couchbase::search::request::SearchRequest;
use std::ops::Deref;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct TestCluster {
    pub cluster_version: NodeVersion,
    pub test_setup_config: TestSetupConfig,
    inner: Cluster,
}

impl Deref for TestCluster {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestCluster {
    pub fn default_bucket(&self) -> &str {
        &self.test_setup_config.default_bucket
    }

    pub fn default_scope(&self) -> &str {
        &self.test_setup_config.default_scope
    }

    pub fn default_collection(&self) -> &str {
        &self.test_setup_config.default_collection
    }
}

impl TestCluster {
    pub async fn new(cluster_version: NodeVersion, test_setup_config: TestSetupConfig) -> Self {
        let inner = test_setup_config.setup_cluster().await;

        Self {
            cluster_version,
            test_setup_config,
            inner,
        }
    }

    pub fn bucket(&self, name: impl Into<String>) -> TestBucket {
        TestBucket::new(self.inner.bucket(name))
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        timeout(Duration::from_secs(15), self.inner.query(statement, opts))
            .await
            .unwrap()
    }

    pub fn buckets(&self) -> TestBucketManager {
        TestBucketManager::new(self.inner.buckets())
    }

    pub fn users(&self) -> TestUserManager {
        TestUserManager::new(self.inner.users())
    }

    pub async fn ping(
        &self,
        opts: impl Into<Option<couchbase::options::diagnostic_options::PingOptions>>,
    ) -> error::Result<couchbase::results::diagnostics::PingReport> {
        timeout(Duration::from_secs(15), self.inner.ping(opts))
            .await
            .unwrap()
    }

    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<couchbase::options::diagnostic_options::WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        timeout(Duration::from_secs(15), self.inner.wait_until_ready(opts))
            .await
            .unwrap()
    }
}
