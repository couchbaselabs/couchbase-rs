use couchbase::cluster::Cluster;

pub mod default_cluster_options;
pub mod test_config;

pub async fn create_cluster_from_test_config() -> Cluster {
    let config = {
        let guard = test_config::TEST_CONFIG.read().unwrap();
        guard.clone().unwrap()
    };

    Cluster::connect(
        config.conn_str.clone(),
        default_cluster_options::create_default_options(),
    )
    .await
    .unwrap()
}
