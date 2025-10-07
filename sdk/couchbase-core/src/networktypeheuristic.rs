use crate::parsedconfig::{ParsedConfig, ParsedConfigNodeAddresses};

pub(crate) struct NetworkTypeHeuristic {}

impl NetworkTypeHeuristic {
    fn node_contains_address(node: &ParsedConfigNodeAddresses, addr: &str) -> bool {
        if let Some(p) = node.non_ssl_ports.kv {
            if format!("{}:{}", node.hostname, p) == addr {
                return true;
            }
        }
        if let Some(p) = node.non_ssl_ports.mgmt {
            if format!("{}:{}", node.hostname, p) == addr {
                return true;
            }
        }
        if let Some(p) = node.ssl_ports.kv {
            if format!("{}:{}", node.hostname, p) == addr {
                return true;
            }
        }
        if let Some(p) = node.ssl_ports.kv {
            if format!("{}:{}", node.hostname, p) == addr {
                return true;
            }
        }
        false
    }

    pub fn identify(config: &ParsedConfig) -> String {
        for node in &config.nodes {
            if Self::node_contains_address(&node.addresses, &config.source_hostname) {
                return "default".to_string();
            }
        }

        for node in &config.nodes {
            for (network_type, alt_addrs) in &node.alt_addresses {
                if Self::node_contains_address(alt_addrs, &config.source_hostname) {
                    return network_type.clone();
                }
            }
        }

        "default".to_string()
    }
}
