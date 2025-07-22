use crate::httpx::request::OnBehalfOfInfo;
use crate::service_type::ServiceType;
use std::fmt::Display;

#[derive(Debug, Default, Clone)]
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
