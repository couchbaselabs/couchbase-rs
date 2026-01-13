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
use crate::analyticsx;
use crate::analyticsx::query_options::{Format, PlanFormat, ScanConsistency};
use crate::httpx::request::OnBehalfOfInfo;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AnalyticsOptions {
    pub args: Option<Vec<Value>>,
    pub client_context_id: Option<String>,
    pub format: Option<Format>,
    pub pretty: Option<bool>,
    pub query_context: Option<String>,
    pub read_only: Option<bool>,
    pub scan_consistency: Option<ScanConsistency>,
    pub scan_wait: Option<Duration>,
    pub statement: Option<String>,
    pub timeout: Option<Duration>,
    pub named_args: Option<HashMap<String, Value>>,
    pub raw: Option<HashMap<String, Value>>,

    pub plan_format: Option<PlanFormat>,
    pub logical_plan: Option<bool>,
    pub optimized_logical_plan: Option<bool>,
    pub expression_tree: Option<bool>,
    pub rewritten_expression_tree: Option<bool>,
    pub job: Option<bool>,
    pub max_warnings: Option<i32>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for AnalyticsOptions {
    fn default() -> Self {
        Self {
            args: None,
            client_context_id: None,
            format: None,
            pretty: None,
            query_context: None,
            read_only: None,
            scan_consistency: None,
            scan_wait: None,
            statement: None,
            timeout: None,
            named_args: None,
            raw: None,
            plan_format: None,
            logical_plan: None,
            optimized_logical_plan: None,
            expression_tree: None,
            rewritten_expression_tree: None,
            job: None,
            max_warnings: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }
}

impl AnalyticsOptions {
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

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<Option<String>>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

impl From<AnalyticsOptions> for analyticsx::query_options::QueryOptions {
    fn from(opts: AnalyticsOptions) -> Self {
        analyticsx::query_options::QueryOptions {
            args: opts.args,
            client_context_id: opts.client_context_id,
            format: opts.format,
            pretty: opts.pretty,
            query_context: opts.query_context,
            read_only: opts.read_only,
            scan_consistency: opts.scan_consistency,
            scan_wait: opts.scan_wait,
            statement: opts.statement,
            timeout: opts.timeout,
            named_args: opts.named_args,
            raw: opts.raw,
            plan_format: opts.plan_format,
            logical_plan: opts.logical_plan,
            optimized_logical_plan: opts.optimized_logical_plan,
            expression_tree: opts.expression_tree,
            rewritten_expression_tree: opts.rewritten_expression_tree,
            job: opts.job,
            max_warnings: opts.max_warnings,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetPendingMutationsOptions<'a> {
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,

    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for GetPendingMutationsOptions<'_> {
    fn default() -> Self {
        Self {
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }
}

impl<'a> GetPendingMutationsOptions<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<&'a OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<Option<String>>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

impl<'a> From<&GetPendingMutationsOptions<'a>>
    for analyticsx::query_options::GetPendingMutationsOptions<'a>
{
    fn from(opts: &GetPendingMutationsOptions<'a>) -> Self {
        analyticsx::query_options::GetPendingMutationsOptions {
            on_behalf_of: opts.on_behalf_of,
        }
    }
}
