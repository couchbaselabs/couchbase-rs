use log::error;
use uuid::Uuid;

pub mod consistency_utils;
pub mod default_cluster_options;
pub mod doc_generation;
pub mod features;
pub mod node_version;
mod test_binary_collection;
mod test_bucket;
mod test_cluster;
mod test_collection;
pub mod test_config;
mod test_manager;
mod test_query_index_manager;
mod test_scope;
mod test_search_index_manager;

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
        match f().await {
            Ok(Some(r)) => return r,
            Ok(None) => {}
            Err(e) => {
                error!("{:?}", e);
            }
        };

        tokio::time::sleep(sleep).await;
    }
    panic!("{}", fail_msg.as_ref());
}
