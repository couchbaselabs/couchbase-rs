use std::sync::Arc;
use std::time::Duration;

use crate::authenticator::Authenticator;
use couchbase_core::agentoptions::CompressionConfig;
use typed_builder::TypedBuilder;

pub use couchbase_core::agentoptions::CompressionMode;
use couchbase_core::ondemand_agentmanager::OnDemandAgentManagerOptions;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct ClusterOptions {
    // authenticator specifies the authenticator to use with the cluster.
    #[builder(!default)]
    pub authenticator: Authenticator,
    // timeout_options specifies various operation timeouts.
    pub timeout_options: TimeoutOptions,
    // compression_mode specifies compression related configuration options.
    pub compression_mode: CompressionMode,
    pub retry_strategy: Option<Arc<dyn crate::retry::RetryStrategy>>,
}

#[derive(Clone, Debug, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TimeoutOptions {
    pub kv_connect_timeout: Option<Duration>,
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            kv_connect_timeout: Some(Duration::from_secs(10)),
        }
    }
}

impl From<ClusterOptions> for OnDemandAgentManagerOptions {
    fn from(opts: ClusterOptions) -> Self {
        let builder = OnDemandAgentManagerOptions::builder()
            .authenticator(opts.authenticator)
            .connect_timeout(
                opts.timeout_options
                    .kv_connect_timeout
                    .unwrap_or(Duration::from_secs(10)),
            )
            .compression_config(
                CompressionConfig::builder()
                    .mode(opts.compression_mode)
                    .build(),
            );

        builder.build()
    }
}
