use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::setup_tests;
use couchbase_core::agent::Agent;
use couchbase_core::analyticsoptions::AnalyticsOptions;
use couchbase_core::analyticsx::query_respreader::Status;
use futures::StreamExt;
use serde_json::Value;

mod common;

#[tokio::test]
async fn test_analytics_basic() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let agent = Agent::new(agent_opts).await.unwrap();
    let opts = AnalyticsOptions::new("FROM RANGE(0, 999) AS i SELECT *");

    let mut res = agent.analytics(opts).await.unwrap();

    let mut rows = vec![];
    while let Some(row) = res.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(1000, rows.len());

    let row = rows.first().unwrap();

    let row_value: Value = serde_json::from_slice(row).unwrap();
    let row_obj = row_value.as_object().unwrap();

    assert_eq!(0, row_obj.get("i").unwrap().as_u64().unwrap());

    let meta = res.metadata().unwrap();

    assert!(meta.request_id.is_some());
    assert!(meta.client_context_id.is_none());
    assert_eq!(Status::Success, meta.status.clone().unwrap());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(1000, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!("{\"*\":\"*\"}", meta.signature.as_ref().unwrap().get());
}
