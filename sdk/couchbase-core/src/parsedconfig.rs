use crate::tracingcomponent::ClusterLabels;
use crate::vbucketmap::VbucketMap;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum BucketType {
    Invalid,
    Couchbase,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ParsedConfigNodePorts {
    pub kv: Option<i64>,
    pub mgmt: i64,
    pub query: Option<i64>,
    pub search: Option<i64>,
    pub analytics: Option<i64>,
    // TODO: Do we need views?
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ParsedConfigNodeAddresses {
    pub hostname: String,
    pub non_ssl_ports: ParsedConfigNodePorts,
    pub ssl_ports: ParsedConfigNodePorts,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ParsedConfigNode {
    pub has_data: bool,
    pub addresses: ParsedConfigNodeAddresses,
    pub alt_addresses: HashMap<String, ParsedConfigNodeAddresses>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ParsedConfigFeature {
    FtsVectorSearch,
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct NetworkConfigNode {
    pub node_id: String,
    pub hostname: String,
    pub has_data: bool,
    pub non_ssl_ports: ParsedConfigNodePorts,
    pub ssl_ports: ParsedConfigNodePorts,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct NetworkConfig {
    pub nodes: Vec<NetworkConfigNode>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum ParsedConfigBucketFeature {
    CreateAsDeleted,
    ReplaceBodyWithXattr,
    RangeScan,
    ReplicaRead,
    NonDedupedHistory,
    ReviveDocument,
    Unknown,
}

impl From<String> for ParsedConfigBucketFeature {
    fn from(s: String) -> Self {
        match s.as_str() {
            "tombstonedUserXAttrs" => ParsedConfigBucketFeature::CreateAsDeleted,
            "subdoc.ReplaceBodyWithXattr" => ParsedConfigBucketFeature::ReplaceBodyWithXattr,
            "rangeScan" => ParsedConfigBucketFeature::RangeScan,
            "subdoc.ReplicaRead" => ParsedConfigBucketFeature::ReplicaRead,
            "nonDedupedHistory" => ParsedConfigBucketFeature::NonDedupedHistory,
            "subdoc.ReviveDocument" => ParsedConfigBucketFeature::ReviveDocument,
            _ => ParsedConfigBucketFeature::Unknown,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ParsedConfigBucket {
    pub bucket_uuid: String,
    pub bucket_name: String,
    pub bucket_type: BucketType,
    pub vbucket_map: Option<VbucketMap>,
    pub features: Vec<ParsedConfigBucketFeature>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ParsedConfig {
    pub rev_id: i64,
    pub rev_epoch: i64,

    pub source_hostname: String,

    pub bucket: Option<ParsedConfigBucket>,

    pub nodes: Vec<ParsedConfigNode>,

    pub features: Vec<ParsedConfigFeature>,
    pub cluster_labels: Option<ClusterLabels>,
}

impl Default for ParsedConfig {
    fn default() -> Self {
        Self {
            rev_id: -1,
            rev_epoch: 0,
            source_hostname: "".to_string(),
            bucket: None,
            nodes: vec![],
            features: vec![],
            cluster_labels: None,
        }
    }
}

impl ParsedConfig {
    pub fn is_versioned(&self) -> bool {
        self.rev_epoch > 0 && self.rev_id > 0
    }

    pub fn addresses_group_for_network_type(&self, network_type: &str) -> NetworkConfig {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let node_id = format!(
                "ep-{}-{}",
                node.addresses.hostname, node.addresses.non_ssl_ports.mgmt
            );

            let node_info = if network_type == "default" {
                NetworkConfigNode {
                    node_id,
                    hostname: node.addresses.hostname.clone(),
                    has_data: node.has_data,
                    non_ssl_ports: node.addresses.non_ssl_ports.clone(),
                    ssl_ports: node.addresses.ssl_ports.clone(),
                }
            } else if let Some(alt_info) = node.alt_addresses.get(network_type) {
                NetworkConfigNode {
                    node_id,
                    hostname: alt_info.hostname.clone(),
                    has_data: node.has_data,
                    non_ssl_ports: alt_info.non_ssl_ports.clone(),
                    ssl_ports: alt_info.ssl_ports.clone(),
                }
            } else {
                NetworkConfigNode {
                    node_id,
                    hostname: "".to_string(),
                    has_data: node.has_data,
                    non_ssl_ports: ParsedConfigNodePorts::default(),
                    ssl_ports: ParsedConfigNodePorts::default(),
                }
            };

            nodes.push(node_info);
        }

        NetworkConfig { nodes }
    }
}

impl PartialOrd for ParsedConfig {
    fn partial_cmp(&self, other: &ParsedConfig) -> Option<Ordering> {
        match self.rev_epoch.cmp(&other.rev_epoch) {
            Ordering::Less => {
                return Some(Ordering::Less);
            }
            Ordering::Greater => {
                return Some(Ordering::Greater);
            }
            Ordering::Equal => {}
        }
        match self.rev_id.cmp(&other.rev_id) {
            Ordering::Less => {
                return Some(Ordering::Less);
            }
            Ordering::Greater => {
                return Some(Ordering::Greater);
            }
            Ordering::Equal => {}
        }

        Some(Ordering::Equal)
    }
}
