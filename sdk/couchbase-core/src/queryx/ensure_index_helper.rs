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

use crate::httpx::client::Client;
use crate::httpx::request::OnBehalfOfInfo;
use crate::mgmtx::node_target::NodeTarget;
use crate::queryx::error;
use crate::queryx::query::Query;
use crate::queryx::query_options::{EnsureIndexPollOptions, GetAllIndexesOptions};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EnsureIndexHelper<'a> {
    pub user_agent: &'a str,
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,

    pub index_name: &'a str,
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,

    confirmed_endpoints: Vec<&'a str>,
}

#[derive(Copy, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[non_exhaustive]
pub enum DesiredState {
    Created,
    Deleted,
}

impl<'a> EnsureIndexHelper<'a> {
    pub fn new(
        user_agent: &'a str,
        index_name: &'a str,
        bucket_name: &'a str,
        scope_name: Option<&'a str>,
        collection_name: Option<&'a str>,
        on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    ) -> Self {
        Self {
            user_agent,
            on_behalf_of_info,
            index_name,
            bucket_name,
            scope_name,
            collection_name,
            confirmed_endpoints: vec![],
        }
    }

    async fn poll_one<C: Client>(
        &self,
        client: Arc<C>,
        target: &NodeTarget,
    ) -> error::Result<bool> {
        let resp = Query {
            http_client: client,
            user_agent: self.user_agent.to_string(),
            endpoint: target.endpoint.to_string(),
            auth: target.auth.clone(),
            tracing: Default::default(),
        }
        .get_all_indexes(&GetAllIndexesOptions {
            bucket_name: self.bucket_name,
            scope_name: self.scope_name,
            collection_name: self.collection_name,
            on_behalf_of: self.on_behalf_of_info,
        })
        .await?;

        for index in resp {
            // Indexes here should already be scoped to the bucket, scope, and collection.
            if index.name == self.index_name {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn poll<C: Client>(
        &mut self,
        opts: &'a EnsureIndexPollOptions<C>,
    ) -> error::Result<bool> {
        let mut filtered_targets = Vec::with_capacity(opts.targets.len());

        for target in &opts.targets {
            if !self.confirmed_endpoints.contains(&target.endpoint.as_str()) {
                filtered_targets.push(target);
            }
        }

        let mut success_endpoints = Vec::new();
        for target in &opts.targets {
            let exists = self.poll_one(opts.client.clone(), target).await?;

            match opts.desired_state {
                DesiredState::Created => {
                    if exists {
                        success_endpoints.push(target.endpoint.as_str());
                    }
                }
                DesiredState::Deleted => {
                    if !exists {
                        success_endpoints.push(target.endpoint.as_str());
                    }
                }
            }
        }

        self.confirmed_endpoints
            .extend_from_slice(success_endpoints.as_slice());

        Ok(success_endpoints.len() == filtered_targets.len())
    }
}
