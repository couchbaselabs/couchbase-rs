use crate::common::node_version::NodeVersion;
use crate::common::test_config::TestCluster;

const SERVER_VERSION_722: NodeVersion = NodeVersion {
    major: 7,
    minor: 2,
    patch: 2,
    build: 0,
    edition: None,
    modifier: None,
};

const SERVER_VERSION_762: NodeVersion = NodeVersion {
    major: 7,
    minor: 6,
    patch: 2,
    build: 0,
    edition: None,
    modifier: None,
};

const SERVER_VERSION_764: NodeVersion = NodeVersion {
    major: 7,
    minor: 6,
    patch: 4,
    build: 0,
    edition: None,
    modifier: None,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum TestFeatureCode {
    KV,
    Search,
    Query,
    QueryManagement,
    SearchManagement,
    SearchManagementCollections,
    BucketManagement,
    CollectionNoExpiry,
    CollectionUpdates,
    ClusterLabels,
}

impl TestCluster {
    pub fn supports_feature(&self, code: &TestFeatureCode) -> bool {
        match code {
            TestFeatureCode::KV => true,
            TestFeatureCode::Search => true,
            TestFeatureCode::Query => true,
            TestFeatureCode::BucketManagement => true,
            TestFeatureCode::QueryManagement => true,
            TestFeatureCode::SearchManagement => true,
            TestFeatureCode::SearchManagementCollections => {
                !self.cluster_version.lower(&SERVER_VERSION_762)
            }
            TestFeatureCode::CollectionNoExpiry => !self.cluster_version.lower(&SERVER_VERSION_762),
            TestFeatureCode::CollectionUpdates => {
                !self.cluster_version.lower(&SERVER_VERSION_722)
                    && !self.cluster_version.equal(&SERVER_VERSION_722)
            }
            TestFeatureCode::ClusterLabels => !self.cluster_version.lower(&SERVER_VERSION_764),
        }
    }
}
