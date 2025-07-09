use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct ServiceType(InnerServiceType);

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq)]
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

    pub(crate) fn other(val: String) -> ServiceType {
        ServiceType(InnerServiceType::Other(val))
    }
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

impl From<&couchbase_core::service_type::ServiceType> for ServiceType {
    fn from(service_type: &couchbase_core::service_type::ServiceType) -> Self {
        match *service_type {
            couchbase_core::service_type::ServiceType::MEMD => ServiceType::MEMD,
            couchbase_core::service_type::ServiceType::MGMT => ServiceType::MGMT,
            couchbase_core::service_type::ServiceType::QUERY => ServiceType::QUERY,
            couchbase_core::service_type::ServiceType::SEARCH => ServiceType::SEARCH,
            couchbase_core::service_type::ServiceType::EVENTING => ServiceType::EVENTING,
            _ => ServiceType(InnerServiceType::Other(service_type.to_string())),
        }
    }
}
