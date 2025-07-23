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
    Other(String),
}

impl ServiceType {
    pub const MEMD: ServiceType = ServiceType(InnerServiceType::Memd);
    pub const MGMT: ServiceType = ServiceType(InnerServiceType::Mgmt);
    pub const QUERY: ServiceType = ServiceType(InnerServiceType::Query);
    pub const SEARCH: ServiceType = ServiceType(InnerServiceType::Search);
    pub const EVENTING: ServiceType = ServiceType(InnerServiceType::Eventing);
}

impl Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match &self.0 {
            InnerServiceType::Memd => "memd",
            InnerServiceType::Mgmt => "mgmt",
            InnerServiceType::Query => "query",
            InnerServiceType::Search => "search",
            InnerServiceType::Eventing => "eventing",
            InnerServiceType::Other(val) => return write!(f, "unknown({val})"),
        };

        write!(f, "{txt}")
    }
}
