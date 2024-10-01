use rscbx_couchbase_core::crudoptions::UpsertOptions;
use rscbx_couchbase_core::ondemand_agentmanager::{
    OnDemandAgentManager, OnDemandAgentManagerOptions,
};
use rscbx_couchbase_core::queryoptions::QueryOptions;

use crate::common::default_agent_options::create_default_options;
use crate::common::helpers::{generate_key, generate_string_value};
use crate::common::test_config::setup_tests;

mod common;

#[tokio::test]
async fn test_get_cluster_agent() {
    setup_tests();

    let agent_opts = OnDemandAgentManagerOptions::from(create_default_options());

    let mgr = OnDemandAgentManager::new(agent_opts).await.unwrap();

    let agent = mgr.get_cluster_agent().unwrap();

    agent
        .query(
            QueryOptions::builder()
                .statement("SELECT 1=1".to_string())
                .build(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_get_bucket_agent() {
    setup_tests();

    let agent_opts = create_default_options();
    let bucket_name = agent_opts.bucket_name.clone().unwrap();

    let mgr_opts = OnDemandAgentManagerOptions::from(agent_opts);

    let mgr = OnDemandAgentManager::new(mgr_opts).await.unwrap();

    let agent = mgr.get_bucket_agent(bucket_name).await.unwrap();

    let key = generate_key();
    let value = generate_string_value(32);

    let upsert_opts = UpsertOptions::builder()
        .key(key.as_slice())
        .scope_name("")
        .collection_name("")
        .value(value.as_slice())
        .build();

    let upsert_result = agent.upsert(upsert_opts).await.unwrap();

    assert_ne!(0, upsert_result.cas);
}
