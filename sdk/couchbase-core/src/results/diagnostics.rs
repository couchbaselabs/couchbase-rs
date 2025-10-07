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

use crate::connection_state::ConnectionState;
use crate::service_type::ServiceType;
use std::collections::HashMap;

#[derive(Debug)]
pub struct EndpointDiagnostics {
    pub service_type: ServiceType,
    pub id: String,
    pub local_address: Option<String>,
    pub remote_address: String,
    pub last_activity: Option<i64>,
    pub namespace: Option<String>,
    pub state: ConnectionState,
}

#[derive(Debug)]
pub struct DiagnosticsResult {
    pub version: u32,
    pub config_rev: i64,
    pub id: String,
    pub sdk: String,
    pub services: HashMap<ServiceType, Vec<EndpointDiagnostics>>,
}
