extern crate core;

use futures::StreamExt;
use serde_json::Value;

use couchbase_core::agent::Agent;
use couchbase_core::queryoptions::QueryOptions;
use couchbase_core::queryx::query_result::Status;

use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::setup_tests;

mod common;

#[tokio::test]
async fn test_query_basic() {
    setup_tests();

    let agent_opts = create_default_options();

    let mut agent = Agent::new(agent_opts).await.unwrap();
    let opts = QueryOptions::builder()
        .statement("SELECT 1=1".to_string())
        .build();
    let mut res = agent.query(opts).await.unwrap();

    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(1, rows.len());

    let row = rows.first().unwrap();

    let row_value: Value = serde_json::from_slice(row).unwrap();
    let row_obj = row_value.as_object().unwrap();

    assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

    let meta = res.metadata().unwrap();
    assert!(meta.prepared.is_none());
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(Status::Success, meta.status);
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
        meta.signature
            .as_ref()
            .unwrap()
            .get("$1")
            .unwrap()
            .as_str()
            .unwrap()
    );
}

#[tokio::test]
async fn test_prepared_query_basic() {
    setup_tests();

    let agent_opts = create_default_options();

    let mut agent = Agent::new(agent_opts).await.unwrap();
    let opts = QueryOptions::builder()
        .statement("SELECT 1=1".to_string())
        .build();
    let mut res = agent.prepared_query(opts).await.unwrap();

    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(1, rows.len());

    let row = rows.first().unwrap();

    let row_value: Value = serde_json::from_slice(row).unwrap();
    let row_obj = row_value.as_object().unwrap();

    assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

    let meta = res.metadata().unwrap();
    assert!(meta.prepared.is_some());
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(Status::Success, meta.status);
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
        meta.signature
            .as_ref()
            .unwrap()
            .get("$1")
            .unwrap()
            .as_str()
            .unwrap()
    );
}
