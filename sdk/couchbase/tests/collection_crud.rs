use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use bytes::Bytes;
use couchbase::transcoder::{DefaultTranscoder, Transcoder};
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
        .upsert_with_transcoder(&key, value, &DefaultTranscoder {})
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
    let tcoder = YamlTranscoder {};

    collection
        .upsert_with_transcoder(&key, value.clone(), &tcoder)
        .await
        .unwrap();

    let res = collection.get(key).await.unwrap();

    let content: BTreeMap<String, f64> = res.content_as_with_transcoder(&tcoder).unwrap();

    assert_eq!(value, content);
}

struct YamlTranscoder {}

impl Transcoder for YamlTranscoder {
    fn encode<T: Serialize>(&self, value: T) -> couchbase::error::Result<(Bytes, u32)> {
        serde_yaml::to_string(&value)
            .map(|s| (Bytes::from(s), 0x02))
            .map_err(|e| couchbase::error::Error { msg: e.to_string() })
    }

    fn decode<T: DeserializeOwned>(
        &self,
        value: &Bytes,
        _flags: u32,
    ) -> couchbase::error::Result<T> {
        serde_yaml::from_slice(value).map_err(|e| couchbase::error::Error { msg: e.to_string() })
    }
}
