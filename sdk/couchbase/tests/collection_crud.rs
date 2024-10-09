use crate::common::create_cluster_from_test_config;
use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};

mod common;

#[tokio::test]
async fn test_upsert() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket())
        .await
        .scope(test_scope())
        .collection(test_collection());

    collection.upsert("test".to_string(), "test").await.unwrap();
}
