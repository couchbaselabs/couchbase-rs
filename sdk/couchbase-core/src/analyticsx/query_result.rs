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

use std::fmt::{Display, Formatter};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Status {
    Running,
    Success,
    Timeout,
    Fatal,
    Failed,
    Unknown,
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Running => write!(f, "running"),
            Status::Success => write!(f, "success"),
            Status::Timeout => write!(f, "timeout"),
            Status::Fatal => write!(f, "fatal"),
            Status::Failed => write!(f, "failed"),
            Status::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataPlans {
    pub logical_plan: Option<Box<RawValue>>,
    pub optimized_logical_plan: Option<Box<RawValue>>,
    pub rewritten_expression_tree: Option<String>,
    pub expression_tree: Option<String>,
    pub job: Option<Box<RawValue>>,
}

#[derive(Debug, Clone)]
pub struct MetaData {
    pub request_id: Option<String>,
    pub client_context_id: Option<String>,
    pub status: Status,
    pub warnings: Vec<Warning>,
    pub metrics: Option<Metrics>,
    pub signature: Option<Box<RawValue>>,
    pub plans: Option<MetadataPlans>,
}

impl Display for MetaData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request_id: {:?}, client_context_id: {:?}, status: {}, metrics: {:?}, signature: {:?}, warnings: {:?}, plans: {:?}",
            self.request_id,
            self.client_context_id,
            self.status,
            self.metrics,
            self.signature,
            self.warnings,
            self.plans
        )
    }
}

#[derive(Debug, Clone)]
pub struct Warning {
    pub code: u32,
    pub message: String,
}

impl Display for Warning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "code: {}, message: {}", self.code, self.message)
    }
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub elapsed_time: Duration,
    pub execution_time: Duration,
    pub result_count: u64,
    pub result_size: u64,
    pub error_count: u64,
    pub warning_count: u64,
    pub processed_objects: i64,
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "elapsed_time: {:?}, execution_time: {:?}, result_count: {}, result_size: {},  error_count: {}, warning_count: {}, processed_objects: {}",
            self.elapsed_time,
            self.execution_time,
            self.result_count,
            self.result_size,
            self.error_count,
            self.warning_count,
            self.processed_objects,
        )
    }
}
