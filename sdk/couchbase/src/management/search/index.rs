/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use couchbase_core::searchx::index::Index;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// The serialize/deserialize implementation here is not used for serializing/deserializing
// in requests.
// This implementation is intended as a convenience for upserting indexes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SearchIndex {
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

impl SearchIndex {
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

    pub fn params(mut self, params: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn plan_params(mut self, plan_params: impl Into<Option<HashMap<String, Value>>>) -> Self {
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
        source_params: impl Into<Option<HashMap<String, Value>>>,
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

impl From<Index> for SearchIndex {
    fn from(index: Index) -> Self {
        SearchIndex {
            name: index.name,
            index_type: index.index_type,
            params: index.params,
            plan_params: index.plan_params,
            prev_index_uuid: index.prev_index_uuid,
            source_name: index.source_name,
            source_params: index.source_params,
            source_type: index.source_type,
            source_uuid: index.source_uuid,
            uuid: index.uuid,
        }
    }
}

impl From<SearchIndex> for Index {
    fn from(search_index: SearchIndex) -> Self {
        let mut index = Index::new(search_index.name, search_index.index_type);
        index = index.params(search_index.params);
        index = index.plan_params(search_index.plan_params);
        index = index.prev_index_uuid(search_index.prev_index_uuid);
        index = index.source_name(search_index.source_name);
        index = index.source_params(search_index.source_params);
        index = index.source_type(search_index.source_type);
        index = index.source_uuid(search_index.source_uuid);
        index = index.uuid(search_index.uuid);
        index
    }
}
