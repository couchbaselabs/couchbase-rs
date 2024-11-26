use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use crate::common::{create_cluster_from_test_config, new_key};
use bytes::Bytes;
use couchbase::transcoding;
use criterion::{criterion_group, criterion_main, Criterion};
use log::LevelFilter;
use std::time::Duration;

#[path = "../tests/common/mod.rs"]
mod common;

fn upsert(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();

    c.bench_function("upsert", |b| {
        b.to_async(&rt).iter(|| async {
            collection.upsert(&key, "test", None).await.unwrap();
        })
    });
}

fn insert(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();

    c.bench_function("insert", |b| {
        b.to_async(&rt).iter(|| async {
            collection.insert(&key, "test", None).await.unwrap();
        })
    });
}

fn replace(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("replace", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .replace(&key, "test_replaced", None)
                .await
                .unwrap();
        })
    });
}

fn remove(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("remove", |b| {
        b.to_async(&rt).iter(|| async {
            collection.remove(&key, None).await.unwrap();
        })
    });
}

fn exists(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("exists", |b| {
        b.to_async(&rt).iter(|| async {
            collection.exists(&key, None).await.unwrap();
        })
    });
}

fn get(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("get", |b| {
        b.to_async(&rt).iter(|| async {
            collection.get(&key, None).await.unwrap();
        })
    });
}

fn get_and_touch(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("get_and_touch", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .get_and_touch(&key, Duration::from_secs(10), None)
                .await
                .unwrap();
        })
    });
}

fn get_and_lock(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("get_and_lock", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .get_and_lock(&key, Duration::from_secs(10), None)
                .await
                .unwrap();
        })
    });
}

fn unlock(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    let lock_res = rt.block_on(async {
        collection.insert(&key, "test", None).await.unwrap();
        collection
            .get_and_lock(&key, Duration::from_secs(10), None)
            .await
            .unwrap()
    });

    c.bench_function("unlock", |b| {
        b.to_async(&rt).iter(|| async {
            collection.unlock(&key, lock_res.cas(), None).await.unwrap();
        })
    });
}

fn touch(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, "test", None).await.unwrap() });

    c.bench_function("touch", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .touch(&key, Duration::from_secs(10), None)
                .await
                .unwrap();
        })
    });
}

fn append(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async {
        collection
            .insert_raw(
                &key,
                transcoding::raw_binary::encode(Bytes::from("test")).unwrap(),
                None,
            )
            .await
            .unwrap()
    });

    c.bench_function("append", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .binary()
                .append(&key, "append".as_bytes().to_vec(), None)
                .await
                .unwrap();
        })
    });
}

fn prepend(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async {
        collection
            .insert_raw(
                &key,
                transcoding::raw_binary::encode(Bytes::from("test")).unwrap(),
                None,
            )
            .await
            .unwrap()
    });

    c.bench_function("prepend", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .binary()
                .prepend(&key, "prepend".as_bytes().to_vec(), None)
                .await
                .unwrap();
        })
    });
}

fn increment(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, 0, None).await.unwrap() });

    c.bench_function("increment", |b| {
        b.to_async(&rt).iter(|| async {
            collection.binary().increment(&key, None).await.unwrap();
        })
    });
}

fn decrement(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let collection = rt.block_on(async {
        let cluster = create_cluster_from_test_config().await;

        cluster
            .bucket(test_bucket().await)
            .await
            .scope(test_scope().await)
            .collection(test_collection().await)
    });

    let key = new_key();
    rt.block_on(async { collection.insert(&key, 1, None).await.unwrap() });

    c.bench_function("decrement", |b| {
        b.to_async(&rt).iter(|| async {
            collection.binary().decrement(&key, None).await.unwrap();
        })
    });
}

criterion_group!(
    benches,
    upsert,
    // insert,
    replace,
    // remove,
    exists,
    get,
    get_and_touch,
    // get_and_lock,
    // unlock,
    touch,
    append,
    prepend,
    increment,
    decrement
);
criterion_main!(benches);
