use crate::api::MutationState;
use serde::Serializer;
use serde_derive::Serialize;
use serde_json::to_vec;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// Macro to DRY up the repetitive timeout setter.
macro_rules! timeout {
    () => (
        pub fn timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }
    )
}

macro_rules! expiry {
    () => (
        pub fn expiry(mut self, expiry: Duration) -> Self {
            self.expiry = Some(expiry);
            self
        }
    )
}

#[derive(Debug, Default, Serialize)]
pub struct QueryOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_consistency: Option<QueryScanConsistency>,
    #[serde(skip)]
    pub(crate) adhoc: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) timeout: Option<Duration>,
    #[serde(serialize_with = "default_client_context_id")]
    pub(crate) client_context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_parallelism: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_batch: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pipeline_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) scan_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_duration_for_golang")]
    pub(crate) scan_wait: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) readonly: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<QueryProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "convert_mutation_state")]
    pub(crate) consistent_with: Option<MutationState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "args")]
    pub(crate) positional_parameters: Option<Vec<Box<Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    #[serde(serialize_with = "convert_named_params")]
    pub(crate) named_parameters: Option<HashMap<String, Box<Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub(crate) raw: Option<HashMap<String, Box<Value>>>,
    // The statement is not part of the public API, but added here
    // as a convenience so we can conver the whole block into the
    // JSON payload the query engine expects. DO NOT ADD A PUBLIC
    // SETTER!
    pub(crate) statement: Option<String>,
}

fn convert_mutation_state<S>(x: &Option<MutationState>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    todo!("Mutation token conversion still needs to be implemented")
}

fn convert_duration_for_golang<S>(x: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!(
        "{}ms",
        x.expect("Expected a duration!").as_millis()
    ))
}

fn default_client_context_id<S>(x: &Option<String>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if x.is_some() {
        s.serialize_str(x.as_ref().unwrap())
    } else {
        s.serialize_str(&format!("{}", Uuid::new_v4()))
    }
}

fn convert_named_params<S>(x: &Option<HashMap<String, Box<Value>>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        Some(m) => {
            let conv: HashMap<String, &Box<Value>> =
                m.iter().map(|(k, v)| (format!("${}", k), v)).collect();
            s.serialize_some(&conv)
        }
        None => s.serialize_none(),
    }
}

impl QueryOptions {
    timeout!();

    pub fn scan_consistency(mut self, scan_consistency: QueryScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn adhoc(mut self, adhoc: bool) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub fn client_context_id(mut self, client_context_id: String) -> Self {
        self.client_context_id = Some(client_context_id);
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    pub fn pipeline_batch(mut self, pipeline_batch: u32) -> Self {
        self.pipeline_batch = Some(pipeline_batch);
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: u32) -> Self {
        self.pipeline_cap = Some(pipeline_cap);
        self
    }

    pub fn scan_cap(mut self, scan_cap: u32) -> Self {
        self.scan_cap = Some(scan_cap);
        self
    }

    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = Some(readonly);
        self
    }

    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn profile(mut self, profile: QueryProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn consistent_with(mut self, consistent_with: MutationState) -> Self {
        self.consistent_with = Some(consistent_with);
        self
    }

    pub fn positional_parameters(mut self, positional_parameters: Vec<Box<Value>>) -> Self {
        self.positional_parameters = Some(positional_parameters);
        self
    }

    pub fn named_parameters(mut self, named_parameters: HashMap<String, Box<Value>>) -> Self
where {
        self.named_parameters = Some(named_parameters);
        self
    }

    pub fn raw(mut self, raw: HashMap<String, Box<Value>>) -> Self {
        self.raw = Some(raw);
        self
    }
}

#[derive(Debug, Serialize)]
pub enum QueryScanConsistency {
    #[serde(rename = "not_bounded")]
    NotBounded,
    #[serde(rename = "request_plus")]
    RequestPlus,
}

#[derive(Debug, Serialize)]
pub enum QueryProfile {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "phases")]
    Phases,
    #[serde(rename = "timings")]
    Timings,
}

#[derive(Debug, Default)]
pub struct GetOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAndTouchOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndTouchOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct GetAndLockOptions {
    pub(crate) timeout: Option<Duration>,
}

impl GetAndLockOptions {
    timeout!();
}

#[derive(Debug, Default)]
pub struct UpsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
}

impl UpsertOptions {
    timeout!();
    expiry!();
}

#[derive(Debug, Default)]
pub struct InsertOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) expiry: Option<Duration>,
}

impl InsertOptions {
    timeout!();
    expiry!();
}

#[derive(Debug, Default)]
pub struct ReplaceOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
    pub(crate) expiry: Option<Duration>,
}

impl ReplaceOptions {
    timeout!();
    expiry!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct RemoveOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) cas: Option<u64>,
}

impl RemoveOptions {
    timeout!();

    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }
}

#[derive(Debug, Default)]
pub struct ExistsOptions {
    pub(crate) timeout: Option<Duration>,
}

impl ExistsOptions {
    timeout!();
}
