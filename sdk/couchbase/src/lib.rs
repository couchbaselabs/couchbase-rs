extern crate core;

pub mod bucket;
mod capella_ca;
mod clients;
pub mod cluster;
pub mod collection;
pub mod collection_binary_crud;
pub mod collection_crud;
pub mod collection_ds;
pub mod durability_level;
pub mod error;
pub mod error_context;
pub mod management;
pub mod mutation_state;
pub mod options;
pub mod results;
pub mod scope;
pub mod search;
pub mod service_type;
pub mod subdoc;
pub mod transcoding;

pub mod authenticator {
    pub use couchbase_core::authenticator::Authenticator;
    pub use couchbase_core::authenticator::PasswordAuthenticator;
}

pub mod retry {
    use std::sync::Arc;

    use lazy_static::lazy_static;

    pub use couchbase_core::retry::RetryAction;
    pub use couchbase_core::retry::RetryInfo;
    pub use couchbase_core::retry::RetryReason;
    pub use couchbase_core::retry::RetryStrategy;
    pub use couchbase_core::retrybesteffort::BackoffCalculator;
    pub use couchbase_core::retrybesteffort::BestEffortRetryStrategy;
    pub use couchbase_core::retrybesteffort::ExponentialBackoffCalculator;

    lazy_static! {
        pub(crate) static ref DEFAULT_RETRY_STRATEGY: Arc<dyn RetryStrategy> = Arc::new(
            BestEffortRetryStrategy::new(ExponentialBackoffCalculator::default())
        );
    }
}
