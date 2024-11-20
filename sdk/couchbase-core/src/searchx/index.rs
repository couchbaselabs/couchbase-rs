use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

#[derive(Debug, Serialize, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct Index {
    #[builder(!default)]
    pub name: String,
    #[builder(!default)]
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
