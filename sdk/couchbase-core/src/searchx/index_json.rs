use crate::searchx::index;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub(crate) struct SearchIndexResponseJson {
    pub status: String,
    #[serde(rename = "indexDef")]
    pub index_def: IndexJson,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SearchIndexDefsJson {
    #[serde(rename = "implVersion")]
    pub impl_version: String,
    #[serde(rename = "indexDefs")]
    pub index_defs: HashMap<String, IndexJson>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct SearchIndexesResponseJson {
    pub status: String,
    #[serde(rename = "indexDef")]
    pub indexes: SearchIndexDefsJson,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IndexJson {
    pub name: String,
    #[serde(rename = "type")]
    pub index_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_params: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_index_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_params: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

impl From<IndexJson> for index::Index {
    fn from(value: IndexJson) -> index::Index {
        index::Index {
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

impl From<index::Index> for IndexJson {
    fn from(value: index::Index) -> IndexJson {
        IndexJson {
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
