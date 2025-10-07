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

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub struct Index {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub using: String,
    #[serde(default)]
    pub state: String,
    pub is_primary: Option<bool>,
    pub keyspace_id: Option<String>,
    pub namespace_id: Option<String>,
    pub index_key: Option<Vec<String>>,
    pub condition: Option<String>,
    pub partition: Option<String>,
    pub scope_id: Option<String>,
    pub bucket_id: Option<String>,
}
