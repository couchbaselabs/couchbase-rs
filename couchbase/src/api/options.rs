use crate::api::MutationState;
use serde::Serialize;
use serde_json::to_vec;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

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

#[derive(Debug, Default)]
pub struct QueryOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) scan_consistency: Option<QueryScanConsistency>,
    pub(crate) adhoc: Option<bool>,
    pub(crate) client_context_id: Option<String>,
    pub(crate) max_parallelism: Option<u32>,
    pub(crate) pipeline_batch: Option<u32>,
    pub(crate) pipeline_cap: Option<u32>,
    pub(crate) scan_cap: Option<u32>,
    pub(crate) scan_wait: Option<Duration>,
    pub(crate) readonly: Option<bool>,
    pub(crate) metrics: Option<bool>,
    pub(crate) profile: Option<QueryProfile>,
    pub(crate) consistent_with: Option<MutationState>,
    pub(crate) positional_parameters: Option<Vec<Vec<u8>>>,
    pub(crate) named_parameters: Option<HashMap<String, Vec<u8>>>,
    pub(crate) raw: Option<HashMap<String, Vec<u8>>>,
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

    pub fn positional_parameters<T>(mut self, positional_parameters: Vec<Value>) -> Self {
        self.positional_parameters = Some(
            positional_parameters
                .iter()
                .map(|v| to_vec(v).expect("Could not encode positional parameter!"))
                .collect(),
        );
        self
    }

    pub fn named_parameters<T>(mut self, named_parameters: HashMap<String, Value>) -> Self
    where
        T: Serialize,
    {
        self.named_parameters = Some(
            named_parameters
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        to_vec(&v).expect("Could not encode positional parameter!"),
                    )
                })
                .collect(),
        );
        self
    }

    pub fn raw(mut self, raw: HashMap<String, Value>) -> Self {
        self.raw = Some(
            raw.into_iter()
                .map(|(k, v)| (k, to_vec(&v).expect("Could not encode raw value!")))
                .collect(),
        );
        self
    }
}

#[derive(Debug)]
pub enum QueryScanConsistency {
    NotBounded,
    RequestPlus,
}

#[derive(Debug)]
pub enum QueryProfile {
    Off,
    Phases,
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
