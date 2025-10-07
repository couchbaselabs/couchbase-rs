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

use serde::Serialize;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub struct ServiceType(InnerServiceType);

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq, Serialize)]
pub(crate) enum InnerServiceType {
    Kv,
    Mgmt,
    Query,
    Search,
    Eventing,
    Other(String),
}

impl ServiceType {
    pub const KV: ServiceType = ServiceType(InnerServiceType::Kv);
    pub const MGMT: ServiceType = ServiceType(InnerServiceType::Mgmt);
    pub const QUERY: ServiceType = ServiceType(InnerServiceType::Query);
    pub const SEARCH: ServiceType = ServiceType(InnerServiceType::Search);
    pub const EVENTING: ServiceType = ServiceType(InnerServiceType::Eventing);
}

impl Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match &self.0 {
            InnerServiceType::Kv => "kv",
            InnerServiceType::Mgmt => "mgmt",
            InnerServiceType::Query => "query",
            InnerServiceType::Search => "search",
            InnerServiceType::Eventing => "eventing",
            InnerServiceType::Other(val) => return write!(f, "unknown({val})"),
        };

        write!(f, "{txt}")
    }
}

impl From<&couchbase_core::service_type::ServiceType> for ServiceType {
    fn from(service_type: &couchbase_core::service_type::ServiceType) -> Self {
        match *service_type {
            couchbase_core::service_type::ServiceType::MEMD => ServiceType::KV,
            couchbase_core::service_type::ServiceType::MGMT => ServiceType::MGMT,
            couchbase_core::service_type::ServiceType::QUERY => ServiceType::QUERY,
            couchbase_core::service_type::ServiceType::SEARCH => ServiceType::SEARCH,
            couchbase_core::service_type::ServiceType::EVENTING => ServiceType::EVENTING,
            _ => ServiceType(InnerServiceType::Other(service_type.to_string())),
        }
    }
}

impl From<couchbase_core::service_type::ServiceType> for ServiceType {
    fn from(service_type: couchbase_core::service_type::ServiceType) -> Self {
        Self::from(&service_type)
    }
}

impl From<ServiceType> for couchbase_core::service_type::ServiceType {
    fn from(service_type: ServiceType) -> Self {
        match service_type {
            ServiceType::KV => couchbase_core::service_type::ServiceType::MEMD,
            ServiceType::MGMT => couchbase_core::service_type::ServiceType::MGMT,
            ServiceType::QUERY => couchbase_core::service_type::ServiceType::QUERY,
            ServiceType::SEARCH => couchbase_core::service_type::ServiceType::SEARCH,
            ServiceType::EVENTING => couchbase_core::service_type::ServiceType::EVENTING,
            _ => unreachable!(), // This isn't possible, users can't create other, and we just don't.
        }
    }
}
