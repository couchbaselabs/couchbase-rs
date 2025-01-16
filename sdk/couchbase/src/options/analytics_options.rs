use couchbase_core::analyticsx;
use serde::Serialize;
use serde_json::Value;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
}

impl From<ScanConsistency> for analyticsx::query_options::ScanConsistency {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => analyticsx::query_options::ScanConsistency::NotBounded,
            ScanConsistency::RequestPlus => analyticsx::query_options::ScanConsistency::RequestPlus,
        }
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct AnalyticsOptions {
    pub(crate) client_context_id: Option<String>,
    pub(crate) priority: Option<bool>,
    pub(crate) read_only: Option<bool>,
    pub(crate) scan_consistency: Option<ScanConsistency>,

    pub(crate) positional_parameters: Option<Vec<Value>>,
    pub(crate) named_parameters: Option<HashMap<String, Value>>,
    pub(crate) raw: Option<HashMap<String, Value>>,

    pub(crate) retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

impl AnalyticsOptions {
    pub fn client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    pub fn priority(mut self, priority: bool) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn add_positional_parameter<T: Serialize>(
        mut self,
        parameters: T,
    ) -> crate::error::Result<Self> {
        let parameters = serde_json::to_value(&parameters)?;

        match self.positional_parameters {
            Some(mut params) => {
                params.push(parameters);
                self.positional_parameters = Some(params);
            }
            None => {
                self.positional_parameters = Some(vec![parameters]);
            }
        }
        Ok(self)
    }

    pub fn add_named_parameter<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> crate::error::Result<Self> {
        let value = serde_json::to_value(&value)?;

        match self.named_parameters {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
        }
        Ok(self)
    }

    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> crate::error::Result<Self> {
        let value = serde_json::to_value(&value)?;

        match self.raw {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
        }
        Ok(self)
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn crate::retry::RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
