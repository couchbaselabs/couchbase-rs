use crate::error;
use crate::error::Error;
use crate::mutation_state::MutationState;
use couchbase_core::options::query;
use couchbase_core::queryx;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
    AtPlus(MutationState),
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum ReplicaLevel {
    On,
    Off,
}

impl From<ReplicaLevel> for queryx::query_options::ReplicaLevel {
    fn from(rl: ReplicaLevel) -> Self {
        match rl {
            ReplicaLevel::On => queryx::query_options::ReplicaLevel::On,
            ReplicaLevel::Off => queryx::query_options::ReplicaLevel::Off,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ProfileMode {
    Off,
    Phases,
    Timings,
}

impl From<ProfileMode> for queryx::query_options::ProfileMode {
    fn from(pm: ProfileMode) -> Self {
        match pm {
            ProfileMode::Off => queryx::query_options::ProfileMode::Off,
            ProfileMode::Phases => queryx::query_options::ProfileMode::Phases,
            ProfileMode::Timings => queryx::query_options::ProfileMode::Timings,
        }
    }
}

#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct QueryOptions {
    pub ad_hoc: Option<bool>,
    pub client_context_id: Option<String>,
    pub flex_index: Option<bool>,
    pub max_parallelism: Option<u32>,
    pub metrics: Option<bool>,
    pub named_parameters: Option<HashMap<String, Value>>,
    pub pipeline_batch: Option<u32>,
    pub pipeline_cap: Option<u32>,
    pub positional_parameters: Option<Vec<Value>>,
    pub preserve_expiry: Option<bool>,
    pub profile: Option<ProfileMode>,
    pub raw: Option<HashMap<String, Value>>,
    pub read_only: Option<bool>,
    pub scan_cap: Option<u32>,
    pub scan_consistency: Option<ScanConsistency>,
    pub scan_wait: Option<Duration>,
    pub server_timeout: Option<Duration>,
    pub use_replica: Option<ReplicaLevel>,
}

impl QueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ad_hoc(mut self, ad_hoc: bool) -> Self {
        self.ad_hoc = Some(ad_hoc);
        self
    }

    pub fn client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    pub fn flex_index(mut self, flex_index: bool) -> Self {
        self.flex_index = Some(flex_index);
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn add_named_parameter<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.named_parameters {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
        }
        Ok(self)
    }

    pub fn pipeline_batch(mut self, pipeline_batch: u32) -> Self {
        self.pipeline_batch = Some(pipeline_batch);
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: u32) -> Self {
        self.pipeline_cap = Some(pipeline_cap);
        self
    }

    pub fn add_positional_parameter<T: Serialize>(mut self, parameters: T) -> error::Result<Self> {
        let parameters =
            serde_json::to_value(&parameters).map_err(Error::encoding_failure_from_serde)?;

        match self.positional_parameters {
            Some(mut params) => {
                params.push(parameters);
                self.positional_parameters = Some(params);
            }
            None => {
                self.positional_parameters = Some(vec![parameters]);
            }
        }
        Ok(self)
    }

    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    pub fn profile(mut self, profile: ProfileMode) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.raw {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
        }
        Ok(self)
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    pub fn scan_cap(mut self, scan_cap: u32) -> Self {
        self.scan_cap = Some(scan_cap);
        self
    }

    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    pub fn use_replica(mut self, use_replica: ReplicaLevel) -> Self {
        self.use_replica = Some(use_replica);
        self
    }
}

impl TryFrom<QueryOptions> for query::QueryOptions {
    type Error = error::Error;

    fn try_from(opts: QueryOptions) -> Result<query::QueryOptions, Self::Error> {
        let (mutation_state, scan_consistency) = match opts.scan_consistency {
            Some(ScanConsistency::AtPlus(state)) => (
                Some(state.into()),
                Some(queryx::query_options::ScanConsistency::AtPlus),
            ),
            Some(ScanConsistency::NotBounded) => (
                None,
                Some(queryx::query_options::ScanConsistency::NotBounded),
            ),
            Some(ScanConsistency::RequestPlus) => (
                None,
                Some(queryx::query_options::ScanConsistency::RequestPlus),
            ),
            None => (None, None),
        };

        let mut builder = query::QueryOptions::new()
            .args(opts.positional_parameters)
            .client_context_id(opts.client_context_id)
            .max_parallelism(opts.max_parallelism)
            .metrics(opts.metrics.unwrap_or_default())
            .pipeline_batch(opts.pipeline_batch)
            .pipeline_cap(opts.pipeline_cap)
            .preserve_expiry(opts.preserve_expiry)
            .profile(opts.profile.map(|p| p.into()))
            .read_only(opts.read_only)
            .scan_cap(opts.scan_cap)
            .scan_consistency(scan_consistency)
            .scan_wait(opts.scan_wait)
            .sparse_scan_vectors(mutation_state)
            .timeout(opts.server_timeout)
            .use_replica(opts.use_replica.map(|r| r.into()))
            .named_args(opts.named_parameters)
            .raw(opts.raw);

        Ok(builder)
    }
}
