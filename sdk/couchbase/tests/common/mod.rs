use couchbase::cluster::Cluster;
use uuid::Uuid;

pub mod default_cluster_options;
pub mod doc_generation;
pub mod test_config;

pub async fn create_cluster_from_test_config() -> Cluster {
    let config = {
        let guard = test_config::TEST_CONFIG.read().await;
        guard.clone().unwrap()
    };

    Cluster::connect(
        config.conn_str.clone(),
        default_cluster_options::create_default_options().await,
    )
    .await
    .unwrap()
}

pub fn new_key() -> String {
    Uuid::new_v4().to_string()
}
