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

use crate::retry::RetryStrategy;
use crate::service_type::ServiceType;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct DiagnosticsOptions {}

impl DiagnosticsOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct PingOptions {
    pub service_types: Option<Vec<ServiceType>>,

    pub kv_timeout: Option<std::time::Duration>,
    pub query_timeout: Option<std::time::Duration>,
    pub search_timeout: Option<std::time::Duration>,
}

impl PingOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn service_types(mut self, service_types: Vec<ServiceType>) -> Self {
        self.service_types = Some(service_types);
        self
    }

    pub fn kv_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.kv_timeout = Some(timeout);
        self
    }

    pub fn query_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    pub fn search_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.search_timeout = Some(timeout);
        self
    }
}

#[derive(Copy, Debug, Default, Clone, Eq, PartialEq)]
pub enum ClusterState {
    #[default]
    Online,
    Degraded,
    Offline,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WaitUntilReadyOptions {
    pub desired_state: Option<ClusterState>,
    pub service_types: Option<Vec<ServiceType>>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl Default for WaitUntilReadyOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitUntilReadyOptions {
    pub fn new() -> Self {
        Self {
            desired_state: None,
            service_types: None,
            retry_strategy: None,
        }
    }

    pub fn desired_state(mut self, state: ClusterState) -> Self {
        self.desired_state = Some(state);
        self
    }

    pub fn service_types(mut self, service_types: Vec<ServiceType>) -> Self {
        self.service_types = Some(service_types);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

impl From<ClusterState> for couchbase_core::options::waituntilready::ClusterState {
    fn from(state: ClusterState) -> Self {
        match state {
            ClusterState::Online => couchbase_core::options::waituntilready::ClusterState::Online,
            ClusterState::Degraded => {
                couchbase_core::options::waituntilready::ClusterState::Degraded
            }
            ClusterState::Offline => couchbase_core::options::waituntilready::ClusterState::Offline,
        }
    }
}
