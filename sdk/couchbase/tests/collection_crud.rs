use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use bytes::Bytes;
use couchbase::transcoding;
use couchbase::transcoding::{encode_common_flags, DataType};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

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

    let key = new_key();

    collection.upsert(&key, "test").await.unwrap();

    let res = collection.get(key).await.unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test", content);
}

#[tokio::test]
async fn test_upsert_with_transcoder() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket())
        .await
        .scope(test_scope())
        .collection(test_collection());

    let value = RawValue::from_string(r#"{"test": "test"}"#.to_string()).unwrap();

    let key = new_key();

    collection
        .upsert_raw(&key, transcoding::json::encode(value).unwrap())
        .await
        .unwrap();

    let res = collection.get(key).await.unwrap();

    let content: Box<RawValue> = res.content_as().unwrap();

    assert_eq!(r#"{"test": "test"}"#, content.get());
}

#[tokio::test]
async fn test_upsert_with_custom_transcoder() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket())
        .await
        .scope(test_scope())
        .collection(test_collection());

    let mut value = BTreeMap::new();
    value.insert("x".to_string(), 1.0);
    value.insert("y".to_string(), 2.0);

    let key = new_key();

    collection
        .upsert_raw(
            &key,
            transcoding::RawValue {
                content: Bytes::from(serde_yaml::to_string(&value).unwrap()),
                flags: encode_common_flags(DataType::Binary),
            },
        )
        .await
        .unwrap();

    let res = collection.get(key).await.unwrap();

    let content = res.content_as_raw();
    let content: BTreeMap<String, f64> = serde_yaml::from_slice(&content.content).unwrap();

    assert_eq!(value, content);
}
