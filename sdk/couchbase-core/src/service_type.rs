use std::fmt::{Display, Formatter};

#[non_exhaustive]
pub enum ServiceType {
    Memd,
    Mgmt,
    Query,
    Search,
    Analytics,
    Eventing,
}

impl Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            ServiceType::Memd => "memd",
            ServiceType::Mgmt => "mgmt",
            ServiceType::Query => "query",
            ServiceType::Search => "search",
            ServiceType::Analytics => "analytics",
            ServiceType::Eventing => "eventing",
            _ => "unknown service",
        };

        write!(f, "{}", txt)
    }
}
