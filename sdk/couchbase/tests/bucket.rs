use crate::common::new_key;
use crate::common::test_config::run_test;
use std::ops::Add;

mod common;

#[test]
#[should_panic]
fn test_upsert() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket("idonotexistonthiscluster")
            .scope(cluster.default_scope())
            .collection(cluster.default_collection());

        let key = new_key();

        collection
            .upsert(&key, "test", None)
            .await
            .expect("Expected panic due to timeout");
    })
}
