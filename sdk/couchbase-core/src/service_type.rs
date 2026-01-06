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
#[serde(rename_all = "lowercase")]
pub(crate) enum InnerServiceType {
    Memd,
    Mgmt,
    Query,
    Search,
    Eventing,
    Analytics,
    Other(String),
}

impl ServiceType {
    pub const MEMD: ServiceType = ServiceType(InnerServiceType::Memd);
    pub const MGMT: ServiceType = ServiceType(InnerServiceType::Mgmt);
    pub const QUERY: ServiceType = ServiceType(InnerServiceType::Query);
    pub const SEARCH: ServiceType = ServiceType(InnerServiceType::Search);
    pub const EVENTING: ServiceType = ServiceType(InnerServiceType::Eventing);
    pub const ANALYTICS: ServiceType = ServiceType(InnerServiceType::Analytics);
}

impl Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match &self.0 {
            InnerServiceType::Memd => "memd",
            InnerServiceType::Mgmt => "mgmt",
            InnerServiceType::Query => "query",
            InnerServiceType::Search => "search",
            InnerServiceType::Eventing => "eventing",
            InnerServiceType::Analytics => "analytics",
            InnerServiceType::Other(val) => return write!(f, "unknown({val})"),
        };

        write!(f, "{txt}")
    }
}
