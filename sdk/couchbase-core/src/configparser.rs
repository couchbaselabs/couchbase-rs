use std::collections::HashMap;

use crate::cbconfig::{TerseConfig, TerseExtNodePorts, VBucketServerMap};
use crate::error::Result;
use crate::parsedconfig::{
    BucketType, ParsedConfig, ParsedConfigBucket, ParsedConfigFeatures, ParsedConfigNode,
    ParsedConfigNodeAddresses, ParsedConfigNodePorts,
};
use crate::vbucketmap::VbucketMap;

pub(crate) struct ConfigParser {}

impl ConfigParser {
    pub fn parse_terse_config(config: TerseConfig, source_hostname: &str) -> Result<ParsedConfig> {
        let rev_id = config.rev;
        let rev_epoch = config.rev_epoch.unwrap_or_default();

        let len_nodes = config.nodes.len();
        let mut nodes = Vec::with_capacity(config.nodes_ext.len());
        for (node_idx, node) in config.nodes_ext.into_iter().enumerate() {
            let node_hostname = Self::parse_config_hostname(&node.hostname, source_hostname);

            let mut alt_addresses = HashMap::new();
            for (network_type, alt_addrs) in node.alternate_addresses {
                let alt_hostname = Self::parse_config_hostname(&alt_addrs.hostname, &node_hostname);
                let this_address = Self::parse_config_hosts_into(&alt_hostname, alt_addrs.ports);

                alt_addresses.insert(network_type, this_address);
            }

            let this_node = ParsedConfigNode {
                has_data: node_idx < len_nodes,
                addresses: Self::parse_config_hosts_into(
                    &node_hostname,
                    node.services.unwrap_or_default(),
                ),
                alt_addresses,
            };

            nodes.push(this_node);
        }

        let bucket = if let Some(bucket_name) = config.name {
            let bucket_uuid = config.uuid.unwrap_or_default();
            let (bucket_type, vbucket_map) = match config.node_locator.as_str() {
                "vbucket" => (
                    BucketType::Couchbase,
                    Self::parse_vbucket_server_map(config.vbucket_server_map)?,
                ),
                _ => (BucketType::Invalid, None),
            };

            Some(ParsedConfigBucket {
                bucket_uuid,
                bucket_name,
                bucket_type,
                vbucket_map,
            })
        } else {
            None
        };

        let features = if let Some(caps) = config.cluster_capabilities.get("fts") {
            ParsedConfigFeatures {
                fts_vector_search: caps.contains(&"vectorSearch".to_string()),
            }
        } else {
            ParsedConfigFeatures {
                fts_vector_search: false,
            }
        };

        Ok(ParsedConfig {
            rev_id,
            rev_epoch,
            bucket,
            nodes,
            features,
            source_hostname: source_hostname.to_string(),
        })
    }

    fn parse_config_hostname(hostname: &str, source_hostname: &str) -> String {
        if hostname.is_empty() {
            return source_hostname.to_string();
        }

        if hostname.contains(':') {
            return format!("[{}]", hostname);
        }

        hostname.to_string()
    }

    fn parse_config_hosts_into(
        hostname: &str,
        ports: TerseExtNodePorts,
    ) -> ParsedConfigNodeAddresses {
        ParsedConfigNodeAddresses {
            hostname: hostname.to_string(),
            non_ssl_ports: ParsedConfigNodePorts {
                kv: ports.kv,
                mgmt: ports.mgmt,
                query: ports.n1ql,
                search: ports.fts,
                analytics: ports.cbas,
            },
            ssl_ports: ParsedConfigNodePorts {
                kv: ports.kv_ssl,
                mgmt: ports.mgmt_ssl,
                query: ports.n1ql_ssl,
                search: ports.fts_ssl,
                analytics: ports.cbas_ssl,
            },
        }
    }

    fn parse_vbucket_server_map(
        vbucket_server_map: Option<VBucketServerMap>,
    ) -> Result<Option<VbucketMap>> {
        if let Some(vbucket_server_map) = vbucket_server_map {
            if vbucket_server_map.vbucket_map.is_empty() {
                return Ok(None);
            }

            return Ok(Some(VbucketMap::new(
                vbucket_server_map.vbucket_map,
                vbucket_server_map.num_replicas,
            )?));
        }

        Ok(None)
    }
}
