use crate::durability_level::DurabilityLevel;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct AppendOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct PrependOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct IncrementOptions {
    pub expiry: Option<Duration>,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct DecrementOptions {
    pub expiry: Option<Duration>,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}
