use crate::common::create_cluster_from_test_config;
use crate::common::test_config::setup_tests;
use criterion::{criterion_group, criterion_main, Criterion};
use log::LevelFilter;

#[path = "../tests/common/mod.rs"]
mod common;

fn query(c: &mut Criterion) {
    setup_tests(LevelFilter::Off);

    let rt = tokio::runtime::Runtime::new().unwrap();

    let cluster = rt.block_on(async { create_cluster_from_test_config().await });

    c.bench_function("query", |b| {
        b.to_async(&rt).iter(|| async {
            cluster.query("SELECT 1=1", None).await.unwrap();
        })
    });
}

criterion_group!(benches, query);
criterion_main!(benches);
