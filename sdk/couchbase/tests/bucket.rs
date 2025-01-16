#![feature(async_closure)]

use crate::common::new_key;
use crate::common::test_config::run_test;
use std::ops::Add;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

mod common;

#[test]
#[should_panic]
fn test_upsert() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket("idonotexistonthiscluster")
            .scope(cluster.default_scope)
            .collection(cluster.default_collection);

        let key = new_key();

        timeout_at(
            Instant::now().add(Duration::from_millis(2500)),
            collection.upsert(&key, "test", None),
        )
        .await
        .unwrap()
        .expect("Expected panic due to timeout");
    })
}
