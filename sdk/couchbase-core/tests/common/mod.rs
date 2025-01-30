use couchbase_core::agent::Agent;
use couchbase_core::features::BucketFeature;
use tokio::time::Instant;

pub mod default_agent_options;
pub mod helpers;
pub mod test_config;

pub async fn feature_supported(agent: &Agent, feature: BucketFeature) -> bool {
    agent.bucket_features().await.unwrap().contains(&feature)
}

pub async fn try_until<Fut, T>(
    deadline: Instant,
    sleep: tokio::time::Duration,
    fail_msg: impl AsRef<str>,
    mut f: impl FnMut() -> Fut,
) -> T
where
    Fut: std::future::Future<Output = Result<Option<T>, couchbase_core::error::Error>>,
{
    while Instant::now() < deadline {
        let res = f().await.unwrap();
        if let Some(r) = res {
            return r;
        }
        tokio::time::sleep(sleep).await;
    }
    panic!("{}", fail_msg.as_ref());
}
