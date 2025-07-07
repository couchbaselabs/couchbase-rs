use crate::httpx::request::OnBehalfOfInfo;
use crate::service_type::ServiceType;
use serde::ser::SerializeStruct;
use std::fmt::Display;

pub struct PingOptions {
    pub service_types: Vec<ServiceType>,

    pub kv_timeout: std::time::Duration,
    pub query_timeout: std::time::Duration,
    pub search_timeout: std::time::Duration,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
}
