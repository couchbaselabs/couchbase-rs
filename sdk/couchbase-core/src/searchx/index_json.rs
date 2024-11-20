use crate::searchx::index;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    pub name: String,
    pub index_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Box<RawValue>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_params: Option<HashMap<String, Box<RawValue>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_index_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_params: Option<HashMap<String, Box<RawValue>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

impl From<index::Index> for Index {
    fn from(value: index::Index) -> Index {
        Index {
            name: value.name,
            index_type: value.index_type,

            params: value.params,
            plan_params: value.plan_params,
            prev_index_uuid: value.prev_index_uuid,
            source_name: value.source_name,
            source_params: value.source_params,
            source_type: value.source_type,
            source_uuid: value.source_uuid,
            uuid: value.uuid,
        }
    }
}
