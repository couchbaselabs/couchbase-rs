use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Index {
    pub name: String,
    pub index_type: String,

    pub params: Option<HashMap<String, Box<RawValue>>>,
    pub plan_params: Option<HashMap<String, Box<RawValue>>>,
    pub prev_index_uuid: Option<String>,
    pub source_name: Option<String>,
    pub source_params: Option<HashMap<String, Box<RawValue>>>,
    pub source_type: Option<String>,
    pub source_uuid: Option<String>,
    pub uuid: Option<String>,
}

impl Index {
    pub fn new(name: impl Into<String>, index_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index_type: index_type.into(),
            params: None,
            plan_params: None,
            prev_index_uuid: None,
            source_name: None,
            source_params: None,
            source_type: None,
            source_uuid: None,
            uuid: None,
        }
    }

    pub fn params(mut self, params: impl Into<Option<HashMap<String, Box<RawValue>>>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn plan_params(
        mut self,
        plan_params: impl Into<Option<HashMap<String, Box<RawValue>>>>,
    ) -> Self {
        self.plan_params = plan_params.into();
        self
    }

    pub fn prev_index_uuid(mut self, prev_index_uuid: impl Into<Option<String>>) -> Self {
        self.prev_index_uuid = prev_index_uuid.into();
        self
    }

    pub fn source_name(mut self, source_name: impl Into<Option<String>>) -> Self {
        self.source_name = source_name.into();
        self
    }

    pub fn source_params(
        mut self,
        source_params: impl Into<Option<HashMap<String, Box<RawValue>>>>,
    ) -> Self {
        self.source_params = source_params.into();
        self
    }

    pub fn source_type(mut self, source_type: impl Into<Option<String>>) -> Self {
        self.source_type = source_type.into();
        self
    }

    pub fn source_uuid(mut self, source_uuid: impl Into<Option<String>>) -> Self {
        self.source_uuid = source_uuid.into();
        self
    }

    pub fn uuid(mut self, uuid: impl Into<Option<String>>) -> Self {
        self.uuid = uuid.into();
        self
    }
}
