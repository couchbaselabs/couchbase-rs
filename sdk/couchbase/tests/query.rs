use crate::common::create_cluster_from_test_config;
use crate::common::test_config::setup_tests;
use couchbase::options::query_options::QueryOptions;
use couchbase::results::query_results::QueryStatus;
use futures::StreamExt;
use serde_json::Value;

mod common;

#[tokio::test]
async fn test_query_basic() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let opts = QueryOptions::builder().metrics(true).build();
    let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(1, rows.len());

    let row = rows.first().unwrap();

    let row_value: Value = serde_json::from_slice(row).unwrap();
    let row_obj = row_value.as_object().unwrap();

    assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

    let meta = res.metadata().await.unwrap();
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(QueryStatus::Success, meta.status);
    assert!(meta.profile.is_none());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(1, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.mutation_count);
    assert_eq!(0, meta.metrics.sort_count);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!(
        "boolean",
        meta.signature.unwrap().get("$1").unwrap().as_str().unwrap()
    );
}

#[tokio::test]
async fn test_prepared_query_basic() {
    setup_tests();

    let cluster = create_cluster_from_test_config().await;

    let opts = QueryOptions::builder().metrics(true).build();
    let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(1, rows.len());

    let row = rows.first().unwrap();

    let row_value: Value = serde_json::from_slice(row).unwrap();
    let row_obj = row_value.as_object().unwrap();

    assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

    let meta = res.metadata().await.unwrap();
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(QueryStatus::Success, meta.status);
    assert!(meta.profile.is_none());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(1, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.mutation_count);
    assert_eq!(0, meta.metrics.sort_count);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!(
        "boolean",
        meta.signature.unwrap().get("$1").unwrap().as_str().unwrap()
    );
}
