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

use std::collections::HashMap;
use std::time::Duration;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

use crate::helpers;
use crate::httpx::request::OnBehalfOfInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Format {
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum PlanFormat {
    Json,
    String,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryOptions {
    pub(crate) args: Option<Vec<Value>>,
    pub(crate) client_context_id: Option<String>,
    pub(crate) format: Option<Format>,
    pub(crate) pretty: Option<bool>,
    pub(crate) query_context: Option<String>,
    pub(crate) read_only: Option<bool>,
    pub(crate) scan_consistency: Option<ScanConsistency>,
    pub(crate) scan_wait: Option<Duration>,
    pub(crate) statement: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) named_args: Option<HashMap<String, Value>>,
    pub(crate) raw: Option<HashMap<String, Value>>,

    pub(crate) plan_format: Option<PlanFormat>,
    pub(crate) logical_plan: Option<bool>,
    pub(crate) optimized_logical_plan: Option<bool>,
    pub(crate) expression_tree: Option<bool>,
    pub(crate) rewritten_expression_tree: Option<bool>,
    pub(crate) job: Option<bool>,
    pub(crate) max_warnings: Option<i32>,

    pub(crate) on_behalf_of: Option<OnBehalfOfInfo>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn args(mut self, args: impl Into<Option<Vec<Value>>>) -> Self {
        self.args = args.into();
        self
    }

    pub fn client_context_id(mut self, client_context_id: impl Into<Option<String>>) -> Self {
        self.client_context_id = client_context_id.into();
        self
    }

    pub fn pretty(mut self, pretty: impl Into<Option<bool>>) -> Self {
        self.pretty = pretty.into();
        self
    }

    pub fn query_context(mut self, query_context: impl Into<Option<String>>) -> Self {
        self.query_context = query_context.into();
        self
    }

    pub fn read_only(mut self, read_only: impl Into<Option<bool>>) -> Self {
        self.read_only = read_only.into();
        self
    }

    pub fn scan_consistency(
        mut self,
        scan_consistency: impl Into<Option<ScanConsistency>>,
    ) -> Self {
        self.scan_consistency = scan_consistency.into();
        self
    }

    pub fn scan_wait(mut self, scan_wait: impl Into<Option<Duration>>) -> Self {
        self.scan_wait = scan_wait.into();
        self
    }

    pub fn statement(mut self, statement: impl Into<Option<String>>) -> Self {
        self.statement = statement.into();
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }

    pub fn named_args(mut self, named_args: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.named_args = named_args.into();
        self
    }

    pub fn raw(mut self, raw: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.raw = raw.into();
        self
    }

    pub fn plan_format(mut self, plan_format: impl Into<Option<PlanFormat>>) -> Self {
        self.plan_format = plan_format.into();
        self
    }

    pub fn logical_plan(mut self, logical_plan: impl Into<Option<bool>>) -> Self {
        self.logical_plan = logical_plan.into();
        self
    }

    pub fn optimized_logical_plan(
        mut self,
        optimized_logical_plan: impl Into<Option<bool>>,
    ) -> Self {
        self.optimized_logical_plan = optimized_logical_plan.into();
        self
    }

    pub fn expression_tree(mut self, expression_tree: impl Into<Option<bool>>) -> Self {
        self.expression_tree = expression_tree.into();
        self
    }

    pub fn rewritten_expression_tree(
        mut self,
        rewritten_expression_tree: impl Into<Option<bool>>,
    ) -> Self {
        self.rewritten_expression_tree = rewritten_expression_tree.into();
        self
    }

    pub fn job(mut self, job: impl Into<Option<bool>>) -> Self {
        self.job = job.into();
        self
    }

    pub fn max_warnings(mut self, max_warnings: impl Into<Option<i32>>) -> Self {
        self.max_warnings = max_warnings.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

impl Serialize for QueryOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use helpers::durations;
        let mut map = serializer.serialize_map(None)?;

        macro_rules! serialize_if_not_none {
            ($field:expr, $name:expr) => {
                if !$field.is_none() {
                    map.serialize_entry($name, &$field)?;
                }
            };
        }

        macro_rules! serialize_duration_if_not_none {
            ($field:expr, $name:expr) => {
                if let Some(f) = $field {
                    map.serialize_entry($name, &durations::duration_to_golang_string(&f))?;
                }
            };
        }

        serialize_if_not_none!(self.args, "args");
        serialize_if_not_none!(self.client_context_id, "client_context_id");
        serialize_if_not_none!(self.format, "format");
        serialize_if_not_none!(self.pretty, "pretty");
        serialize_if_not_none!(self.query_context, "query_context");
        serialize_if_not_none!(self.read_only, "readonly");
        serialize_if_not_none!(self.scan_consistency, "scan_consistency");
        serialize_duration_if_not_none!(self.scan_wait, "scan_wait");
        serialize_if_not_none!(self.statement, "statement");
        serialize_duration_if_not_none!(self.timeout, "timeout");

        serialize_if_not_none!(self.plan_format, "plan_format");
        serialize_if_not_none!(self.logical_plan, "logical_plan");
        serialize_if_not_none!(self.optimized_logical_plan, "optimized_logical_plan");
        serialize_if_not_none!(self.expression_tree, "expression_tree");
        serialize_if_not_none!(self.rewritten_expression_tree, "rewritten_expression_tree");
        serialize_if_not_none!(self.job, "job");
        serialize_if_not_none!(self.max_warnings, "max_warnings");

        if let Some(args) = &self.named_args {
            // Prefix each named_arg with "$" if not already prefixed.
            for (key, value) in args {
                let key = if key.starts_with('$') {
                    key
                } else {
                    &format!("${key}")
                };
                map.serialize_entry(key, value)?;
            }
        }

        if let Some(raw) = &self.raw {
            // Move raw fields to the top level.
            for (key, value) in raw {
                map.serialize_entry(key, value)?;
            }
        }

        map.end()
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PingOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> PingOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct GetPendingMutationsOptions<'a> {
    pub(crate) on_behalf_of: Option<&'a OnBehalfOfInfo>,
}

impl<'a> GetPendingMutationsOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }
}
