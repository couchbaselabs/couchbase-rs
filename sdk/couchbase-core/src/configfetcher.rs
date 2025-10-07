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

use crate::cbconfig;
use crate::configparser::ConfigParser;
use crate::configwatcher::ConfigWatcherMemd;
use crate::error::Error;
use crate::kvclient::{KvClient, StdKvClient};
use crate::kvclient_ops::KvClientOps;
use crate::kvclientmanager::{KvClientManager, KvClientManagerClientType};
use crate::kvclientpool::KvClientPool;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::request::{GetClusterConfigKnownVersion, GetClusterConfigRequest};
use crate::parsedconfig::ParsedConfig;
use log::{debug, trace};
use std::env;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ConfigFetcherMemd<M: KvClientManager> {
    kv_client_manager: Arc<M>,
}

impl<M: KvClientManager> ConfigFetcherMemd<M> {
    pub fn new(kv_client_manager: Arc<M>) -> Self {
        Self { kv_client_manager }
    }
    pub(crate) async fn poll_one(
        &self,
        endpoint: &str,
        rev_id: i64,
        rev_epoch: i64,
        skip_fetch_cb: impl FnOnce(Arc<KvClientManagerClientType<M>>) -> bool,
    ) -> crate::error::Result<Option<ParsedConfig>> {
        let client = self.kv_client_manager.get_client(endpoint).await?;

        if skip_fetch_cb(client.clone()) {
            return Ok(None);
        }

        debug!("Fetching config from {}", &endpoint);

        let hostname = client.remote_hostname();
        let known_version = {
            if rev_id > 0 && client.has_feature(HelloFeature::ClusterMapKnownVersion) {
                Some(GetClusterConfigKnownVersion { rev_epoch, rev_id })
            } else {
                None
            }
        };

        let resp = client
            .get_cluster_config(GetClusterConfigRequest { known_version })
            .await
            .map_err(Error::new_contextual_memdx_error)?;

        if resp.config.is_empty() {
            return Ok(None);
        }

        let config = cbconfig::parse::parse_terse_config(&resp.config, hostname)?;

        if env::var("RSCBC_DEBUG_CONFIG").is_ok() {
            trace!("Fetcher fetched new config {:?}", &config);
        }

        Ok(Some(ConfigParser::parse_terse_config(config, hostname)?))
    }
}
