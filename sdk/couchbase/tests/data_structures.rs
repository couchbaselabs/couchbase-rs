use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use log::LevelFilter;

mod common;

#[tokio::test]
async fn test_list() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);
    let key = new_key();

    let list = collection.list(&key, None);

    list.append("test2").await.unwrap();
    list.prepend("test1").await.unwrap();

    let index = list.position("test2".to_string()).await.unwrap();
    assert_eq!(index, 1);

    let size = list.len().await.unwrap();
    assert_eq!(size, 2);

    let mut iter = list.iter::<String>().await.unwrap();
    assert_eq!(Some("test1".to_string()), iter.next());
    assert_eq!(Some("test2".to_string()), iter.next());
    assert_eq!(None, iter.next());

    list.remove(0).await.unwrap();

    let item: String = list.get(0).await.unwrap();
    assert_eq!(item, "test2");

    list.clear().await.unwrap();

    let res = collection.exists(key, None).await.unwrap();
    assert!(!res.exists());
}

#[tokio::test]
async fn test_map() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);
    let key = new_key();

    let map = collection.map(&key, None);

    map.insert("foo", "test2".to_string()).await.unwrap();
    map.insert("bar", "test4".to_string()).await.unwrap();

    let res: String = map.get("bar").await.unwrap();
    assert_eq!(res, "test4");

    let res = map.contains_key("bar").await.unwrap();
    assert!(res);

    let res = map.len().await.unwrap();
    assert_eq!(res, 2);

    let mut res = map.keys().await.unwrap();
    res.sort();
    assert_eq!(res, vec!["bar", "foo"]);

    let mut res: Vec<String> = map.values().await.unwrap();
    res.sort();
    assert_eq!(res, vec!["test2", "test4"]);

    let mut res: Vec<(String, String)> = map.iter::<String>().await.unwrap().collect::<Vec<_>>();
    res.sort();

    assert_eq!(
        res,
        vec![
            ("bar".to_string(), "test4".to_string()),
            ("foo".to_string(), "test2".to_string()),
        ]
    );

    map.remove("foo").await.unwrap();

    let res = map.contains_key("foo").await.unwrap();
    assert!(!res);

    map.clear().await.unwrap();

    let res = collection.exists(key, None).await.unwrap();
    assert!(!res.exists());
}

#[tokio::test]
async fn test_set() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);
    let key = new_key();

    let set = collection.set(&key, None);

    set.insert("test1").await.unwrap();
    set.insert("test2").await.unwrap();
    set.insert("test2").await.unwrap();

    let res = set.len().await.unwrap();
    assert_eq!(res, 2);

    let res: Vec<String> = set.values().await.unwrap();
    assert_eq!(res, vec!["test1", "test2"]);

    let res = set.contains("test1".to_string()).await.unwrap();
    assert!(res);

    let mut iter = set.iter::<String>().await.unwrap();
    assert_eq!(Some("test1".to_string()), iter.next());
    assert_eq!(Some("test2".to_string()), iter.next());
    assert_eq!(None, iter.next());

    set.remove("test1".to_string()).await.unwrap();

    let res = set.contains("test1".to_string()).await.unwrap();
    assert!(!res);

    set.clear().await.unwrap();

    let res = collection.exists(key, None).await.unwrap();
    assert!(!res.exists());
}

#[tokio::test]
async fn test_queue() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let collection = cluster
        .bucket(test_bucket().await)
        .scope(test_scope().await)
        .collection(test_collection().await);
    let key = new_key();

    let queue = collection.queue(&key, None);

    queue.push("test1").await.unwrap();
    queue.push("test2").await.unwrap();

    let res = queue.len().await.unwrap();
    assert_eq!(res, 2);

    let mut iter = queue.iter::<String>().await.unwrap();
    assert_eq!(Some("test1".to_string()), iter.next());
    assert_eq!(Some("test2".to_string()), iter.next());
    assert_eq!(None, iter.next());

    let res: String = queue.pop().await.unwrap();
    assert_eq!(res, "test1");

    queue.clear().await.unwrap();

    let res = collection.exists(key, None).await.unwrap();
    assert!(!res.exists());
}
