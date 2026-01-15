/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::common::node_version::{NodeEdition, NodeVersion};
use crate::common::test_cluster::TestCluster;

const SERVER_VERSION_720: NodeVersion = NodeVersion {
    major: 7,
    minor: 2,
    patch: 0,
    build: 0,
    edition: None,
    modifier: None,
};

const SERVER_VERSION_760: NodeVersion = NodeVersion {
    major: 7,
    minor: 6,
    patch: 0,
    build: 0,
    edition: None,
    modifier: None,
};

const SERVER_VERSION_800: NodeVersion = NodeVersion {
    major: 8,
    minor: 0,
    patch: 0,
    build: 0,
    edition: None,
    modifier: None,
};

const SERVER_VERSION_800_COMMUNITY: NodeVersion = NodeVersion {
    major: 8,
    minor: 0,
    patch: 0,
    build: 0,
    edition: Some(NodeEdition::Community),
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
    BucketManagementCompressionMode,
    BucketManagementConflictResolutionType,
    CollectionMaxExpiry,
    CollectionUpdateMaxExpiry,
    HistoryRetention,
    VectorSearch,
    UserGroups,
    UsersMB69096,
}

impl TestCluster {
    pub fn supports_feature(&self, code: &TestFeatureCode) -> bool {
        match code {
            TestFeatureCode::KV => true,
            TestFeatureCode::Search => true,
            TestFeatureCode::Query => true,
            TestFeatureCode::BucketManagement => true,
            TestFeatureCode::BucketManagementCompressionMode => {
                self.cluster_version.edition != Some(NodeEdition::Community)
            }
            TestFeatureCode::BucketManagementConflictResolutionType => {
                self.cluster_version.edition != Some(NodeEdition::Community)
            }
            TestFeatureCode::QueryManagement => true,
            TestFeatureCode::SearchManagement => true,
            TestFeatureCode::SearchManagementCollections => {
                !self.cluster_version.lower(&SERVER_VERSION_760)
            }
            TestFeatureCode::CollectionMaxExpiry => {
                self.cluster_version.edition != Some(NodeEdition::Community)
            }
            TestFeatureCode::CollectionUpdateMaxExpiry => {
                self.cluster_version.edition != Some(NodeEdition::Community)
                    && !self.cluster_version.lower(&SERVER_VERSION_760)
            }
            TestFeatureCode::HistoryRetention => {
                self.cluster_version.edition != Some(NodeEdition::Community)
                    && !self.cluster_version.lower(&SERVER_VERSION_720)
            }
            TestFeatureCode::VectorSearch => {
                self.cluster_version.edition != Some(NodeEdition::Community)
                    && !self.cluster_version.lower(&SERVER_VERSION_760)
            }
            TestFeatureCode::UserGroups => {
                self.cluster_version.edition != Some(NodeEdition::Community)
            }
            TestFeatureCode::UsersMB69096 => {
                !self.cluster_version.equal(&SERVER_VERSION_800_COMMUNITY)
            }
        }
    }
}
