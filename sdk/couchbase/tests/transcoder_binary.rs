use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
#[cfg(feature = "binary-transcoder")]
use couchbase::transcoder_binary::BinaryTranscoder;

mod common;

#[cfg(feature = "binary-transcoder")]
#[tokio::test]
async fn test_binary_transcoder() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket())
        .await
        .scope(test_scope())
        .collection(test_collection());

    let mut value = vec![0, 1, 2, 3, 4, 5];

    let key = new_key();
    let tcoder = BinaryTranscoder {};

    collection
        .upsert_with_transcoder(&key, value.as_slice(), &tcoder)
        .await
        .unwrap();

    let res = collection.get(key).await.unwrap();

    let content: Vec<i32> = res.content_as_with_transcoder(&tcoder).unwrap();

    assert_eq!(value, content);
}
