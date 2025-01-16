use crate::common::test_config::create_test_cluster;
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
mod common;

fn query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cluster = rt.block_on(async { create_test_cluster().await });

    c.bench_function("query", |b| {
        b.to_async(&rt).iter(|| async {
            cluster.query("SELECT 1=1", None).await.unwrap();
        })
    });
}

criterion_group!(benches, query);
criterion_main!(benches);
