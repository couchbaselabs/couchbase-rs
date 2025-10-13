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

use crate::common::default_agent_options::create_default_options;
use crate::common::test_config::{run_test, setup_test};
use couchbase_core::agent::Agent;
use couchbase_core::connection_state::ConnectionState;
use couchbase_core::options::diagnostics::DiagnosticsOptions;
use couchbase_core::options::ping::PingOptions;
use couchbase_core::options::waituntilready::WaitUntilReadyOptions;
use couchbase_core::results::diagnostics::DiagnosticsResult;
use couchbase_core::results::pingreport::PingState;
use couchbase_core::service_type::ServiceType;
use std::future::Future;
use std::ops::Add;
use std::time::Duration;

mod common;

#[test]
fn test_ping() {
    run_test(async |mut agent| {
        let opts = PingOptions::new()
            .kv_timeout(Duration::from_millis(1000))
            .query_timeout(Duration::from_millis(75000))
            .search_timeout(Duration::from_millis(75000));

        let report = agent.ping(&opts).await.unwrap();

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

#[test]
fn test_diagnostics_before_connections_ready() {
    setup_test(async |config| {
        let agent_opts = create_default_options(config.clone()).await;

        let mut agent = Agent::new(agent_opts).await.unwrap();

        let report = agent.diagnostics(&DiagnosticsOptions::new()).await.unwrap();

        verify_report(
            report,
            config.bucket.clone(),
            |state| {
                assert!(
                    state == ConnectionState::Disconnected || state == ConnectionState::Connecting
                )
            },
            false,
        );
    })
}

#[test]
fn test_diagnostics_after_wait_until_ready() {
    setup_test(async |config| {
        let agent_opts = create_default_options(config.clone()).await;

        let mut agent = Agent::new(agent_opts).await.unwrap();

        agent
            .wait_until_ready(&WaitUntilReadyOptions::new())
            .await
            .unwrap();

        let report = agent.diagnostics(&DiagnosticsOptions::new()).await.unwrap();

        verify_report(
            report,
            config.bucket.clone(),
            |state| assert_eq!(ConnectionState::Connected, state),
            true,
        );
    })
}

fn verify_report(
    report: DiagnosticsResult,
    bucket: String,
    connection_state_ok: fn(ConnectionState),
    has_activity: bool,
) {
    assert_eq!(2, report.version);
    assert!(!report.id.is_empty());
    assert!(report.config_rev > 0);
    assert_eq!("rust", &report.sdk);
    assert_eq!(1, report.services.len());
    let memd = report.services.get(&ServiceType::MEMD).unwrap();

    for report in memd {
        assert!(!report.id.is_empty());
        assert_eq!(bucket, report.namespace.clone().unwrap());
        assert!(!report.remote_address.is_empty());
        if has_activity {
            assert!(report.last_activity.is_some_and(|la| la > 0));
        } else {
            assert!(report.last_activity.is_none());
        }
        connection_state_ok(report.state);
        assert_eq!(ServiceType::MEMD, report.service_type);
    }
}
