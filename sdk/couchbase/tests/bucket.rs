use crate::common::test_config::{setup_tests, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use log::LevelFilter;
use std::ops::Add;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

mod common;

#[tokio::test]
async fn test_upsert() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket("idonotexistonthiscluster")
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    if timeout_at(
        Instant::now().add(Duration::from_millis(2500)),
        collection.upsert(&key, "test", None),
    )
    .await
    .is_ok()
    {
        panic!("expected timeout");
    }
}
