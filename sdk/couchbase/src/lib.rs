pub mod bucket;
mod clients;
pub mod cluster;
pub mod collection;
pub mod collection_crud;
pub mod connstr;
pub mod error;
mod mutation_state;
pub mod options;
pub mod results;
pub mod scope;

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
