use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::RetryStrategy;
use crate::retrybesteffort::BestEffortRetryStrategy;
use crate::service_type::ServiceType;
use std::sync::Arc;

#[derive(Copy, Debug, Default, Clone, Eq, PartialEq)]
pub enum ClusterState {
    #[default]
    Online,
    Degraded,
    Offline,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WaitUntilReadyOptions {
    pub desired_state: Option<ClusterState>,
    pub service_types: Option<Vec<ServiceType>>,
    pub retry_strategy: Arc<dyn RetryStrategy>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
}

impl Default for WaitUntilReadyOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitUntilReadyOptions {
    pub fn new() -> Self {
        Self {
            desired_state: None,
            service_types: None,
            retry_strategy: Arc::new(BestEffortRetryStrategy::default()),
            on_behalf_of: None,
        }
    }

    pub fn desired_state(mut self, state: ClusterState) -> Self {
        self.desired_state = Some(state);
        self
    }

    pub fn service_types(mut self, service_types: Vec<ServiceType>) -> Self {
        self.service_types = Some(service_types);
        self
    }

    pub fn on_behalf_of(mut self, info: OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(info);
        self
    }
}
