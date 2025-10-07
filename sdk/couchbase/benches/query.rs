/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::common::test_config::create_test_cluster;
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
mod common;

fn query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cluster = rt.block_on(async { create_test_cluster().await });
    let scope = cluster
        .bucket(cluster.default_bucket())
        .scope(cluster.default_scope());

    c.bench_function("query", |b| {
        b.to_async(&rt).iter(|| async {
            scope.query("SELECT 1=1", None).await.unwrap();
        })
    });
}

criterion_group!(benches, query);
criterion_main!(benches);
