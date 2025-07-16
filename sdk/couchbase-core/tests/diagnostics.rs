use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::{run_test, setup_test};
use couchbase_core::agent::Agent;
use couchbase_core::options::ping::PingOptions;
use couchbase_core::options::waituntilready::WaitUntilReadyOptions;
use couchbase_core::pingreport::PingState;
use std::future::Future;
use std::ops::Add;
use std::time::Duration;

mod common;

#[test]
fn test_ping() {
    run_test(async |mut agent| {
        let report = agent
            .ping(&PingOptions {
                service_types: None,
                kv_timeout: Duration::from_millis(1000),
                query_timeout: Duration::from_millis(1000),
                search_timeout: Duration::from_millis(1000),
                on_behalf_of: None,
            })
            .await
            .unwrap();

        assert!(report.config_rev > 0);
        assert!(!report.id.is_empty());
        assert_eq!(report.sdk, "rust");
        assert_eq!(report.version, 2);
        assert!(report
            .services
            .contains_key(&couchbase_core::service_type::ServiceType::MEMD));
        assert!(report
            .services
            .contains_key(&couchbase_core::service_type::ServiceType::QUERY));
        assert!(report
            .services
            .contains_key(&couchbase_core::service_type::ServiceType::SEARCH));

        let memd = report
            .services
            .get(&couchbase_core::service_type::ServiceType::MEMD)
            .unwrap();
        assert!(!memd.is_empty());

        for node in memd {
            assert!(node.id.is_some());
            assert_eq!(
                agent.test_setup_config.bucket,
                node.namespace.clone().unwrap()
            );
            assert!(node.error.is_none());
            assert_eq!(PingState::Ok, node.state);
            assert!(!node.latency.is_zero());
            assert!(!node.remote.is_empty());
        }

        let query = report
            .services
            .get(&couchbase_core::service_type::ServiceType::QUERY)
            .unwrap();
        assert!(!query.is_empty());

        for node in query {
            assert!(node.namespace.is_none());
            assert!(node.error.is_none());
            assert_eq!(PingState::Ok, node.state);
            assert!(!node.latency.is_zero());
            assert!(!node.remote.is_empty());
        }

        let search = report
            .services
            .get(&couchbase_core::service_type::ServiceType::SEARCH)
            .unwrap();
        assert!(!search.is_empty());

        for node in search {
            assert!(node.namespace.is_none());
            assert!(node.error.is_none());
            assert_eq!(PingState::Ok, node.state);
            assert!(!node.latency.is_zero());
            assert!(!node.remote.is_empty());
        }
    });
}

#[test]
fn test_wait_until_ready() {
    setup_test(async |config| {
        let agent_opts = create_default_options(config.clone()).await;

        let mut agent = Agent::new(agent_opts).await.unwrap();

        agent
            .wait_until_ready(&WaitUntilReadyOptions::new())
            .await
            .unwrap();
    })
}
