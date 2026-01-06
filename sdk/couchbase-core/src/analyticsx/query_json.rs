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

use crate::analyticsx::query_result::Status;
use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Debug, Deserialize)]
pub struct QueryErrorResponse {
    #[serde(default)]
    pub errors: Vec<QueryError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryMetadataPlans {
    pub logical_plan: Option<Box<RawValue>>,
    pub optimized_logical_plan: Option<Box<RawValue>>,
    pub rewritten_expression_tree: Option<String>,
    pub expression_tree: Option<String>,
    pub job: Option<Box<RawValue>>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    pub request_id: Option<String>,
    #[serde(rename = "clientContextID")]
    pub client_context_id: Option<String>,
    pub status: Status,
    #[serde(default)]
    pub errors: Vec<QueryError>,
    #[serde(default)]
    pub warnings: Vec<QueryWarning>,
    pub metrics: Option<QueryMetrics>,
    pub signature: Option<Box<RawValue>>,
    pub plans: Option<QueryMetadataPlans>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryMetrics {
    pub elapsed_time: Option<String>,
    pub execution_time: Option<String>,
    pub result_count: Option<u64>,
    pub result_size: Option<u64>,
    pub error_count: Option<u64>,
    pub warning_count: Option<u64>,
    pub processed_objects: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryWarning {
    pub code: Option<u32>,
    pub msg: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QueryError {
    pub code: u32,
    pub msg: String,
}
