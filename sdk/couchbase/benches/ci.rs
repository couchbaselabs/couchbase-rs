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
use crate::common::new_key;
use crate::common::test_config::get_bucket;
use couchbase::transcoding;
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "./util.rs"]
mod util;

#[path = "../tests/common/mod.rs"]
mod common;

fn upsert_and_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (cluster, bucket) = get_bucket(&rt);

    let collection = bucket
        .scope(cluster.default_scope())
        .collection(cluster.default_collection());

    let key = new_key();
    let (value, flags) = transcoding::json::encode("test").unwrap();

    c.bench_function("upsert_and_get", |b| {
        b.to_async(&rt).iter(|| async {
            collection
                .upsert_raw(&key, &value, flags, None)
                .await
                .unwrap();
            collection.get(&key, None).await.unwrap();
        })
    });
}

fn query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (cluster, bucket) = get_bucket(&rt);
    let scope = bucket.scope(cluster.default_scope());

    c.bench_function("query", |b| {
        b.to_async(&rt).iter(|| async {
            scope.query("SELECT 1=1", None).await.unwrap();
        })
    });
}

criterion_group!(
    name = benches;
    config = util::configured_criterion();
    targets = upsert_and_get, query
);
criterion_main!(benches);
