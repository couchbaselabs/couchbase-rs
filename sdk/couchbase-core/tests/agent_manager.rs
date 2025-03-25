use couchbase_core::crudoptions::UpsertOptions;
use couchbase_core::ondemand_agentmanager::{OnDemandAgentManager, OnDemandAgentManagerOptions};
use couchbase_core::queryoptions::QueryOptions;

use crate::common::default_agent_options::create_default_options;
use crate::common::helpers::{generate_bytes_value, generate_key};
use crate::common::test_config::setup_test;

mod common;

#[test]
fn test_get_cluster_agent() {
    setup_test(async |config| {
        let agent_opts = OnDemandAgentManagerOptions::from(create_default_options(config).await);

        let mgr = OnDemandAgentManager::new(agent_opts).await.unwrap();

        let agent = mgr.get_cluster_agent();

        agent
            .query(QueryOptions::default().statement("SELECT 1=1".to_string()))
            .await
            .unwrap();
    });
}

#[test]
fn test_get_bucket_agent() {
    setup_test(async |config| {
        let bucket_name = config.bucket.clone();
        let agent_opts = create_default_options(config).await;

        let mgr_opts = OnDemandAgentManagerOptions::from(agent_opts);

        let mgr = OnDemandAgentManager::new(mgr_opts).await.unwrap();

        let agent = mgr.get_bucket_agent(bucket_name).await.unwrap();

        let key = generate_key();
        let value = generate_bytes_value(32);

        let upsert_opts = UpsertOptions::new(key.as_slice(), "", "", value.as_slice());

        let upsert_result = agent.upsert(upsert_opts).await.unwrap();

        assert_ne!(0, upsert_result.cas);
    });
}
