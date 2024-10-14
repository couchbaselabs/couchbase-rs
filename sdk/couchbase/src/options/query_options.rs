use crate::error;
use crate::mutation_state::MutationState;
use couchbase_core::{queryoptions, queryx};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ScanConsistency {
    NotBounded,
    RequestPlus,
    AtPlus,
}

impl From<ScanConsistency> for queryx::query_options::ScanConsistency {
    fn from(sc: ScanConsistency) -> Self {
        match sc {
            ScanConsistency::NotBounded => queryx::query_options::ScanConsistency::NotBounded,
            ScanConsistency::RequestPlus => queryx::query_options::ScanConsistency::RequestPlus,
            ScanConsistency::AtPlus => queryx::query_options::ScanConsistency::AtPlus,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
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

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct QueryOptions {
    pub ad_hoc: Option<bool>,
    pub client_context_id: Option<String>,
    pub consistent_with: Option<MutationState>,
    pub flex_index: Option<bool>,
    pub max_parallelism: Option<u32>,
    pub metrics: Option<bool>,
    pub named_arguments: Option<HashMap<String, Value>>,
    pub pipeline_batch: Option<u32>,
    pub pipeline_cap: Option<u32>,
    pub positional_parameters: Option<Vec<Value>>,
    pub preserve_expiry: Option<bool>,
    pub profile: Option<ProfileMode>,
    pub raw: Option<HashMap<String, Value>>,
    pub read_only: Option<bool>,
    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
    pub scan_cap: Option<u32>,
    pub scan_consistency: Option<ScanConsistency>,
    pub scan_wait: Option<Duration>,
    pub server_timeout: Option<Duration>,
    pub use_replica: Option<ReplicaLevel>,
}

impl TryFrom<QueryOptions> for queryoptions::QueryOptions {
    type Error = error::Error;

    fn try_from(opts: QueryOptions) -> Result<queryoptions::QueryOptions, Self::Error> {
        let (mutation_state, scan_consistency) = if let Some(mutation_state) = opts.consistent_with
        {
            (
                Some(mutation_state.into()),
                Some(queryx::query_options::ScanConsistency::AtPlus),
            )
        } else {
            (None, opts.scan_consistency.map(|sc| sc.into()))
        };

        let named_args = if let Some(named_args) = opts.named_arguments {
            let mut collected = HashMap::default();
            for (k, v) in named_args {
                collected.insert(k, serde_json::to_vec(&v)?);
            }
            Some(collected)
        } else {
            None
        };
        let raw = if let Some(raw) = opts.raw {
            let mut collected = HashMap::default();
            for (k, v) in raw {
                collected.insert(k, serde_json::to_vec(&v)?);
            }
            Some(collected)
        } else {
            None
        };
        let positional_params = if let Some(positional_params) = opts.positional_parameters {
            let mut collected = vec![];
            for v in positional_params {
                collected.push(serde_json::to_vec(&v)?);
            }
            Some(collected)
        } else {
            None
        };

        let mut builder = queryoptions::QueryOptions::builder()
            .args(positional_params)
            .client_context_id(opts.client_context_id)
            .max_parallelism(opts.max_parallelism)
            .metrics(opts.metrics)
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
            .named_args(named_args)
            .raw(raw)
            .retry_strategy(opts.retry_strategy);

        Ok(builder.build())
    }
}
