use couchbase_core::crudoptions::UpsertOptions;
use couchbase_core::ondemand_agentmanager::{OnDemandAgentManager, OnDemandAgentManagerOptions};
use couchbase_core::queryoptions::QueryOptions;

use crate::common::default_agent_options::create_default_options;
use crate::common::helpers::{generate_bytes_value, generate_key};
use crate::common::test_config::{setup_tests, test_bucket};

mod common;

#[tokio::test]
async fn test_get_cluster_agent() {
    setup_tests().await;

    let agent_opts = OnDemandAgentManagerOptions::from(create_default_options().await);

    let mgr = OnDemandAgentManager::new(agent_opts).await.unwrap();

    let agent = mgr.get_cluster_agent().unwrap();

    agent
        .query(QueryOptions::default().statement("SELECT 1=1".to_string()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_bucket_agent() {
    setup_tests().await;

    let agent_opts = create_default_options().await;
    let bucket_name = test_bucket().await;

    let mgr_opts = OnDemandAgentManagerOptions::from(agent_opts);

    let mgr = OnDemandAgentManager::new(mgr_opts).await.unwrap();

    let agent = mgr.get_bucket_agent(bucket_name).await.unwrap();

    let key = generate_key();
    let value = generate_bytes_value(32);

    let upsert_opts = UpsertOptions::new(key.as_slice(), "", "", value.as_slice());

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
}
