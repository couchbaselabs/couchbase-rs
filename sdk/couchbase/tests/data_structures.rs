use crate::common::new_key;
use crate::common::test_config::run_test;

mod common;

#[test]
fn test_list() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(cluster.default_bucket())
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());
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
    })
}

#[test]
fn test_map() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(cluster.default_bucket())
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());
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

        let mut res: Vec<(String, String)> =
            map.iter::<String>().await.unwrap().collect::<Vec<_>>();
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
    })
}

#[test]
fn test_set() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(cluster.default_bucket())
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());
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
    })
}

#[test]
fn test_queue() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(cluster.default_bucket())
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());
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
    })
}
