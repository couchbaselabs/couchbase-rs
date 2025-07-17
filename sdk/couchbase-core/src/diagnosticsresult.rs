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
