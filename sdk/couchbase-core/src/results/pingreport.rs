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

use crate::error::Error;
use crate::service_type::ServiceType;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PingState {
    Ok,
    Timeout,
    Error,
}

#[derive(Debug, Clone)]
pub struct EndpointPingReport {
    pub remote: String,
    pub error: Option<Error>,
    pub latency: Duration,
    pub id: Option<String>,
    pub namespace: Option<String>,
    pub state: PingState,
}

#[derive(Debug, Clone)]
pub struct PingReport {
    pub version: u16,
    pub id: String,
    pub sdk: String,
    pub config_rev: i64,
    pub services: HashMap<ServiceType, Vec<EndpointPingReport>>,
}
