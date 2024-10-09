use std::sync::Arc;

use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct QueryOptions {
    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}
