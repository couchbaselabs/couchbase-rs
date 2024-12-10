use crate::common::create_cluster_from_test_config;
use crate::common::test_config::setup_tests;
use couchbase::options::analytics_options::AnalyticsOptions;
use couchbase::results::analytics_results::{AnalyticsMetaData, AnalyticsStatus};
use futures::StreamExt;
use log::LevelFilter;
use serde_json::Value;

mod common;

#[tokio::test]
async fn test_cluster_query_basic() {
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let opts = AnalyticsOptions::builder().build();
    let mut res = cluster
        .analytics_query("FROM RANGE(0, 999) AS i SELECT *", &opts)
        .await
        .unwrap();

    let mut rows: Vec<Value> = vec![];
    while let Some(row) = res.rows().next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(rows.len(), 1000);

    let row = rows.first().unwrap();

    let row_obj = row.as_object().unwrap();

    assert_eq!(0, row_obj.get("i").unwrap().as_i64().unwrap());

    let meta = res.metadata().await.unwrap();
    assert_metadata(meta);
}

fn assert_metadata(meta: AnalyticsMetaData) {
    assert!(!meta.request_id.unwrap().is_empty());
    assert!(!meta.client_context_id.unwrap().is_empty());
    assert_eq!(AnalyticsStatus::Success, meta.status.unwrap());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(1000, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!(r#"{"*":"*"}"#, meta.signature.unwrap().get());
}
