use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use bytes::Bytes;
use couchbase::options::kv_binary_options::{DecrementOptions, IncrementOptions};
use couchbase::subdoc::lookup_in_specs::{GetSpecOptions, LookupInSpec};
use couchbase::subdoc::macros::{LookupInMacros, MutateInMacros};
use couchbase::subdoc::mutate_in_specs::MutateInSpec;
use couchbase::transcoding;
use couchbase::transcoding::{encode_common_flags, DataType};
use log::LevelFilter;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_upsert() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.upsert(&key, "test", None).await.unwrap();

    let res = collection.get(key, None).await.unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test", content);
}

#[tokio::test]
async fn test_upsert_with_transcoder() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let value = RawValue::from_string(r#"{"test": "test"}"#.to_string()).unwrap();

    let key = new_key();

    collection
        .upsert_raw(&key, transcoding::json::encode(value).unwrap(), None)
        .await
        .unwrap();

    let res = collection.get(key, None).await.unwrap();

    let content: Box<RawValue> = res.content_as().unwrap();

    assert_eq!(r#"{"test": "test"}"#, content.get());
}

#[tokio::test]
async fn test_upsert_with_custom_transcoder() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

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
            None,
        )
        .await
        .unwrap();

    let res = collection.get(key, None).await.unwrap();

    let content = res.content_as_raw();
    let content: BTreeMap<String, f64> = serde_yaml::from_slice(&content.content).unwrap();

    assert_eq!(value, content);
}

#[tokio::test]
async fn test_insert() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    let res = collection.get(key, None).await.unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test", content);
}

#[tokio::test]
async fn test_replace() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();
    collection
        .replace(&key, "test_replaced", None)
        .await
        .unwrap();

    let res = collection.get(key, None).await.unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test_replaced", content);
}

#[tokio::test]
async fn test_remove() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();
    collection.remove(&key, None).await.unwrap();

    let res = collection.get(key, None).await;

    assert!(res.is_err());
}

#[tokio::test]
async fn test_exists() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    let res = collection.exists(&key, None).await.unwrap();

    assert!(res.exists());

    collection.remove(&key, None).await.unwrap();

    let res = collection.exists(&key, None).await.unwrap();
    assert!(!res.exists());
}

#[tokio::test]
async fn test_get_and_touch() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    let res = collection
        .get_and_touch(&key, Duration::from_secs(10), None)
        .await
        .unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test", content);
}

#[tokio::test]
async fn test_get_and_lock() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    let res = collection
        .get_and_lock(&key, Duration::from_secs(10), None)
        .await
        .unwrap();

    let content: String = res.content_as().unwrap();

    assert_eq!("test", content);
}

#[tokio::test]
async fn test_unlock() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    let lock_res = collection
        .get_and_lock(&key, Duration::from_secs(10), None)
        .await
        .unwrap();

    collection.unlock(&key, lock_res.cas(), None).await.unwrap();
}

#[tokio::test]
async fn test_touch() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, "test", None).await.unwrap();

    collection
        .touch(&key, Duration::from_secs(10), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_append() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection
        .insert_raw(
            &key,
            transcoding::raw_binary::encode(Bytes::from("test")).unwrap(),
            None,
        )
        .await
        .unwrap();

    collection
        .binary()
        .append(&key, "append".as_bytes().to_vec(), None)
        .await
        .unwrap();

    let res = collection.get(key, None).await.unwrap();

    let raw = res.content_as_raw();
    let content = transcoding::raw_binary::decode(raw).unwrap();

    assert_eq!("testappend", content);
}

#[tokio::test]
async fn test_prepend() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection
        .insert_raw(
            &key,
            transcoding::raw_binary::encode(Bytes::from("test")).unwrap(),
            None,
        )
        .await
        .unwrap();

    collection
        .binary()
        .prepend(&key, "prepend".as_bytes().to_vec(), None)
        .await
        .unwrap();

    let res = collection.get(key, None).await.unwrap();

    let raw = res.content_as_raw();
    let content = transcoding::raw_binary::decode(raw).unwrap();

    assert_eq!("prependtest", content);
}

#[tokio::test]
async fn test_increment() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, 0, None).await.unwrap();

    let res = collection
        .binary()
        .increment(&key, IncrementOptions::builder().delta(1u64).build())
        .await
        .unwrap();

    assert_eq!(1, res.content());
}

#[tokio::test]
async fn test_decrement() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    collection.insert(&key, 1, None).await.unwrap();

    let res = collection
        .binary()
        .decrement(&key, DecrementOptions::builder().delta(1u64).build())
        .await
        .unwrap();

    assert_eq!(0, res.content());
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
struct SubdocObject {
    foo: u32,
    bar: u32,
    baz: String,
    arr: Vec<u32>,
}

#[tokio::test]
async fn test_lookup_in() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    let obj = SubdocObject {
        foo: 14,
        bar: 2,
        baz: "hello".to_string(),
        arr: vec![1, 2, 3],
    };

    collection.upsert(&key, &obj, None).await.unwrap();

    let ops = [
        LookupInSpec::get("baz", None),
        LookupInSpec::exists("not-exists", None),
        LookupInSpec::count("arr", None),
        LookupInSpec::get(
            LookupInMacros::IsDeleted,
            GetSpecOptions::builder().is_xattr(true).build(),
        ),
        LookupInSpec::get("", None),
    ];

    let result = collection.lookup_in(&key, &ops, None).await.unwrap();

    assert_eq!(result.content_as::<String>(0).unwrap(), "hello".to_string());
    assert!(result.exists(0).unwrap());
    assert!(!result.content_as::<bool>(1).unwrap());
    assert!(!result.exists(1).unwrap());
    assert_eq!(result.content_as::<u32>(2).unwrap(), 3);
    assert!(result.exists(2).unwrap());
    assert!(!result.content_as::<bool>(3).unwrap());
    assert!(result.exists(3).unwrap());
    assert_eq!(
        result.content_as_raw(4).unwrap(),
        serde_json::to_vec(&obj).unwrap()
    );
    assert!(result.exists(4).unwrap());
}

#[tokio::test]
async fn test_mutate_in() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);

    let key = new_key();

    let obj = SubdocObject {
        foo: 14,
        bar: 2,
        baz: "hello".to_string(),
        arr: vec![3],
    };

    collection.upsert(&key, &obj, None).await.unwrap();

    let xattr_spec = MutateInSpec::insert("my-cas", MutateInMacros::CAS, None).unwrap();

    assert!(xattr_spec.is_xattr);
    assert!(xattr_spec.expand_macros);

    let ops = [
        MutateInSpec::decrement("bar", 1, None).unwrap(),
        MutateInSpec::increment("bar", 2, None).unwrap(),
        MutateInSpec::upsert("baz", "world", None).unwrap(),
        xattr_spec,
        MutateInSpec::array_prepend("arr", &[1, 2], None).unwrap(),
        MutateInSpec::array_append("arr", &[5, 6], None).unwrap(),
    ];

    let result = collection.mutate_in(&key, &ops, None).await.unwrap();

    assert_eq!(result.entries.len(), 6);
    assert!(result.mutation_token.is_some());
    assert_ne!(result.cas, 0);
    assert_eq!(result.content_as::<u32>(0).unwrap(), 1);
    assert_eq!(result.content_as::<u32>(1).unwrap(), 3);

    let res = collection.get(key, None).await.unwrap();
    let content = res.content_as::<SubdocObject>().unwrap();

    assert_eq!(
        content,
        SubdocObject {
            foo: 14,
            bar: 3,
            baz: "world".to_string(),
            arr: vec![1, 2, 3, 5, 6],
        }
    );
}
