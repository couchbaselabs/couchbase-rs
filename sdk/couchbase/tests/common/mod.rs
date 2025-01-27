use uuid::Uuid;

pub mod consistency_utils;
pub mod default_cluster_options;
pub mod doc_generation;
pub mod features;
pub mod node_version;
pub mod test_config;

use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use tokio::time::Instant;

pub fn new_key() -> String {
    Uuid::new_v4().to_string()
}

pub fn generate_string_value(len: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

pub async fn try_until<Fut, T>(
    deadline: Instant,
    sleep: tokio::time::Duration,
    fail_msg: impl AsRef<str>,
    mut f: impl FnMut() -> Fut,
) -> T
where
    Fut: std::future::Future<Output = Result<Option<T>, couchbase::error::Error>>,
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
