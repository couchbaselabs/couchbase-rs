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
