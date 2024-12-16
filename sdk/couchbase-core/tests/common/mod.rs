use couchbase_core::agent::Agent;
use couchbase_core::features::BucketFeature;

pub mod default_agent_options;
pub mod helpers;
pub mod test_config;

pub async fn feature_supported(agent: &Agent, feature: BucketFeature) -> bool {
    agent.bucket_features().await.unwrap().contains(&feature)
}
