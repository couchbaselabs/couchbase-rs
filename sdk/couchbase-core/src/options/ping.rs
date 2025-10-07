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

use crate::httpx::request::OnBehalfOfInfo;
use crate::service_type::ServiceType;
use std::fmt::Display;

#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct PingOptions {
    pub service_types: Option<Vec<ServiceType>>,

    pub kv_timeout: Option<std::time::Duration>,
    pub query_timeout: Option<std::time::Duration>,
    pub search_timeout: Option<std::time::Duration>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
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

    pub fn on_behalf_of(mut self, info: Option<OnBehalfOfInfo>) -> Self {
        self.on_behalf_of = info;
        self
    }
}
