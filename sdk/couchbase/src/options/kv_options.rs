use crate::durability_level::DurabilityLevel;
use couchbase_core::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct UpsertOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub preserve_expiry: Option<bool>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct InsertOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ReplaceOptions {
    pub expiry: Option<Duration>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct GetOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct ExistsOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct RemoveOptions {
    pub durability_level: Option<DurabilityLevel>,
    pub cas: Option<u64>,
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct GetAndTouchOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct GetAndLockOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct UnlockOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

#[derive(Default, Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into, strip_option)))]
#[non_exhaustive]
pub struct TouchOptions {
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}
