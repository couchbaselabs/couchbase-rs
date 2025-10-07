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

use couchbase_core::ondemand_agentmanager::OnDemandAgentManager;
use couchbase_core::options::crud::UpsertOptions;
use couchbase_core::options::ondemand_agentmanager::OnDemandAgentManagerOptions;
use couchbase_core::options::query::QueryOptions;

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
            .upgrade()
            .unwrap()
            .query(QueryOptions::default().statement("SELECT 1=1".to_string()))
            .await
            .unwrap();
    });
}

#[should_panic]
#[test]
fn test_get_bucket_agent_drop_manager() {
    setup_test(async |config| {
        let agent_opts = OnDemandAgentManagerOptions::from(create_default_options(config).await);

        let agent = {
            let mgr = OnDemandAgentManager::new(agent_opts).await.unwrap();

            let agent = mgr.get_bucket_agent("default").await.unwrap();

            agent
        };

        // There should now be no strong references to the agent, so this should fail.
        agent.upgrade().unwrap();
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

        let upsert_result = agent.upgrade().unwrap().upsert(upsert_opts).await.unwrap();

        assert_ne!(0, upsert_result.cas);
    });
}
