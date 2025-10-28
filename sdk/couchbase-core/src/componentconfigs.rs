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
use crate::address::Address;
use crate::authenticator::Authenticator;
use crate::configmanager::ConfigManagerMemdConfig;
use crate::diagnosticscomponent::DiagnosticsComponentConfig;
use crate::httpx::client::ClientConfig;
use crate::kvclient_babysitter::KvTarget;
use crate::mgmtcomponent::MgmtComponentConfig;
use crate::parsedconfig::{ParsedConfig, ParsedConfigFeature};
use crate::querycomponent::QueryComponentConfig;
use crate::searchcomponent::SearchComponentConfig;
use crate::service_type::ServiceType;
use crate::tls_config::TlsConfig;
use crate::vbucketrouter::VbucketRoutingInfo;
use std::collections::HashMap;
use std::time::Duration;

pub(crate) struct AgentComponentConfigs {
    pub kv_targets: HashMap<String, KvTarget>,
    pub auth: Authenticator,
    pub selected_bucket: Option<String>,

    pub config_manager_memd_config: ConfigManagerMemdConfig,
    pub vbucket_routing_info: VbucketRoutingInfo,
    pub query_config: QueryComponentConfig,
    pub search_config: SearchComponentConfig,
    pub mgmt_config: MgmtComponentConfig,
    pub diagnostics_config: DiagnosticsComponentConfig,
    pub http_client_config: ClientConfig,
}

pub(crate) struct HttpClientConfig {
    pub idle_connection_timeout: Duration,
    pub max_idle_connections_per_host: Option<usize>,
    pub tcp_keep_alive_time: Duration,
}

impl AgentComponentConfigs {
    pub fn gen_from_config(
        config: &ParsedConfig,
        network_type: &str,
        tls_config: Option<TlsConfig>,
        bucket_name: Option<String>,
        authenticator: Authenticator,
        http_client_config: HttpClientConfig,
    ) -> AgentComponentConfigs {
        let rev_id = config.rev_id;
        let network_info = config.addresses_group_for_network_type(network_type);

        let mut gcccp_node_ids = Vec::new();
        let mut kv_data_node_ids = Vec::new();
        let mut kv_data_hosts: HashMap<String, Address> = HashMap::new();
        let mut mgmt_endpoints: HashMap<String, String> = HashMap::new();
        let mut query_endpoints: HashMap<String, String> = HashMap::new();
        let mut search_endpoints: HashMap<String, String> = HashMap::new();

        for node in network_info.nodes {
            let kv_ep_id = format!("kv{}", node.node_id);
            let mgmt_ep_id = format!("mgmt{}", node.node_id);
            let query_ep_id = format!("query{}", node.node_id);
            let search_ep_id = format!("search{}", node.node_id);

            gcccp_node_ids.push(kv_ep_id.clone());

            if node.has_data {
                kv_data_node_ids.push(kv_ep_id.clone());
            }

            if tls_config.is_some() {
                if let Some(p) = node.ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        Address {
                            host: node.hostname.clone(),
                            port: p,
                        },
                    );
                }
                if let Some(p) = node.ssl_ports.mgmt {
                    mgmt_endpoints.insert(mgmt_ep_id, format!("https://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.ssl_ports.query {
                    query_endpoints.insert(query_ep_id, format!("https://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.ssl_ports.search {
                    search_endpoints
                        .insert(search_ep_id, format!("https://{}:{}", node.hostname, p));
                }
            } else {
                if let Some(p) = node.non_ssl_ports.kv {
                    kv_data_hosts.insert(
                        kv_ep_id,
                        Address {
                            host: node.hostname.clone(),
                            port: p,
                        },
                    );
                }
                if let Some(p) = node.non_ssl_ports.mgmt {
                    mgmt_endpoints.insert(mgmt_ep_id, format!("http://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.non_ssl_ports.query {
                    query_endpoints.insert(query_ep_id, format!("http://{}:{}", node.hostname, p));
                }
                if let Some(p) = node.non_ssl_ports.search {
                    search_endpoints
                        .insert(search_ep_id, format!("http://{}:{}", node.hostname, p));
                }
            }
        }

        let mut kv_targets = HashMap::new();
        for (node_id, address) in kv_data_hosts {
            let target = KvTarget {
                address,
                tls_config: tls_config.clone(),
            };

            kv_targets.insert(node_id, target);
        }

        let vbucket_routing_info = if let Some(info) = &config.bucket {
            VbucketRoutingInfo {
                vbucket_info: info.vbucket_map.clone(),
                server_list: kv_data_node_ids,
                bucket_selected: true,
            }
        } else {
            VbucketRoutingInfo {
                vbucket_info: None,
                server_list: kv_data_node_ids,
                bucket_selected: false,
            }
        };

        let mut available_services = vec![ServiceType::MEMD];
        if !query_endpoints.is_empty() {
            available_services.push(ServiceType::QUERY)
        }
        if !search_endpoints.is_empty() {
            available_services.push(ServiceType::SEARCH)
        }

        AgentComponentConfigs {
            kv_targets,
            auth: authenticator.clone(),
            selected_bucket: bucket_name.clone(),
            config_manager_memd_config: ConfigManagerMemdConfig {
                endpoints: gcccp_node_ids,
            },

            vbucket_routing_info,
            query_config: QueryComponentConfig {
                endpoints: query_endpoints,
                authenticator: authenticator.clone(),
            },
            search_config: SearchComponentConfig {
                endpoints: search_endpoints,
                authenticator: authenticator.clone(),
                vector_search_enabled: config
                    .features
                    .contains(&ParsedConfigFeature::FtsVectorSearch),
            },
            http_client_config: ClientConfig {
                tls_config,
                idle_connection_timeout: http_client_config.idle_connection_timeout,
                max_idle_connections_per_host: http_client_config.max_idle_connections_per_host,
                tcp_keep_alive_time: http_client_config.tcp_keep_alive_time,
            },
            mgmt_config: MgmtComponentConfig {
                endpoints: mgmt_endpoints,
                authenticator: authenticator.clone(),
            },
            diagnostics_config: DiagnosticsComponentConfig {
                bucket: bucket_name,
                services: available_services,
                rev_id,
            },
        }
    }
}
