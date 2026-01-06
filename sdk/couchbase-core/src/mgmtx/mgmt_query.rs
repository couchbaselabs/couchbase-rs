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
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct IndexStatusDefinition {
    pub bucket: String,
    pub definition: String,
    pub collection: Option<String>,
    pub scope: Option<String>,
    #[serde(rename = "indexName")]
    pub index_name: String,
    pub status: String,
    #[serde(rename = "storageMode")]
    pub storage_mode: String,
    #[serde(rename = "numReplica")]
    pub replicas: u8,
}

#[derive(Debug, Deserialize)]
pub struct IndexStatus {
    pub indexes: Vec<IndexStatusDefinition>,
}
