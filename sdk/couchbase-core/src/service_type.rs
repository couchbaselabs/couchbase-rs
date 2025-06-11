use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct ServiceType(InnerServiceType);

#[derive(Clone, Debug, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum InnerServiceType {
    Memd,
    Mgmt,
    Query,
    Search,
    Analytics,
    Eventing,
    Other(String),
}

impl ServiceType {
    pub const MEMD: ServiceType = ServiceType(InnerServiceType::Memd);
    pub const MGMT: ServiceType = ServiceType(InnerServiceType::Mgmt);
    pub const QUERY: ServiceType = ServiceType(InnerServiceType::Query);
    pub const SEARCH: ServiceType = ServiceType(InnerServiceType::Search);
    pub const ANALYTICS: ServiceType = ServiceType(InnerServiceType::Analytics);
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
            InnerServiceType::Analytics => "analytics",
            InnerServiceType::Eventing => "eventing",
            InnerServiceType::Other(val) => return write!(f, "unknown({})", val),
        };

        write!(f, "{}", txt)
    }
}
