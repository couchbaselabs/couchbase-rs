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

use crate::service_type::ServiceType;
use std::fmt::Display;

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
}

impl From<WaitUntilReadyOptions>
    for couchbase_core::options::waituntilready::WaitUntilReadyOptions
{
    fn from(options: WaitUntilReadyOptions) -> Self {
        let mut core_opts = Self::new();

        if let Some(state) = options.desired_state {
            core_opts = core_opts.desired_state(state.into());
        }

        if let Some(service_types) = options.service_types {
            core_opts =
                core_opts.service_types(service_types.into_iter().map(|s| s.into()).collect());
        }

        core_opts
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

impl From<DiagnosticsOptions> for couchbase_core::options::diagnostics::DiagnosticsOptions {
    fn from(_options: DiagnosticsOptions) -> Self {
        Self::new()
    }
}

impl From<PingOptions> for couchbase_core::options::ping::PingOptions {
    fn from(options: PingOptions) -> Self {
        let mut core_opts = Self::new();

        if let Some(service_types) = options.service_types {
            core_opts =
                core_opts.service_types(service_types.into_iter().map(|s| s.into()).collect());
        }

        if let Some(timeout) = options.kv_timeout {
            core_opts = core_opts.kv_timeout(timeout);
        }
        if let Some(timeout) = options.query_timeout {
            core_opts = core_opts.query_timeout(timeout);
        }
        if let Some(timeout) = options.search_timeout {
            core_opts = core_opts.search_timeout(timeout);
        }

        core_opts
    }
}
