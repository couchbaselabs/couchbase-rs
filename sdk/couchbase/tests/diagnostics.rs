use crate::common::test_config::{create_test_cluster, run_test};
use couchbase::diagnostics::ConnectionState;
use couchbase::options::diagnostic_options::PingOptions;
use couchbase::results::diagnostics::{PingReport, PingState};
use couchbase::service_type::ServiceType;
use std::time::Duration;

mod common;

#[test]
fn test_cluster_ping() {
    run_test(async |mut cluster| {
        let report = cluster
            .ping(PingOptions {
                service_types: None,
                kv_timeout: Some(Duration::from_millis(1000)),
                query_timeout: Some(Duration::from_millis(1000)),
                search_timeout: Some(Duration::from_millis(1000)),
            })
            .await
            .unwrap();

        verify_ping_report(report, None);
    })
}

#[test]
fn test_bucket_ping() {
    run_test(async |mut cluster| {
        let bucket = cluster.bucket(cluster.default_bucket());

        bucket.wait_until_ready(None).await.unwrap();

        let report = bucket
            .ping(PingOptions {
                service_types: None,
                kv_timeout: Some(Duration::from_millis(1000)),
                query_timeout: Some(Duration::from_millis(1000)),
                search_timeout: Some(Duration::from_millis(1000)),
            })
            .await
            .unwrap();

        verify_ping_report(report, Some(cluster.default_bucket().to_string()));
    })
}

#[test]
fn test_cluster_wait_until_ready() {
    run_test(async |_cluster| {
        let cluster = create_test_cluster().await;

        cluster.wait_until_ready(None).await.unwrap();
    });
}

#[test]
fn test_bucket_wait_until_ready() {
    run_test(async |cluster| {
        let bucket = cluster.bucket(cluster.default_bucket());

        bucket.wait_until_ready(None).await.unwrap();
    });
}

#[test]
fn test_diagnostics() {
    run_test(async |mut cluster| {
        cluster.wait_until_ready(None).await.unwrap();

        let report = cluster
            .diagnostics(None)
            .await
            .expect("Diagnostics request failed");

        assert!(report.config_rev > 0);
        assert!(!report.id.is_empty());
        assert_eq!(report.sdk, "rust");
        assert_eq!(report.version, 2);
        assert!(report.services.contains_key(&ServiceType::KV));

        let memd = report.services.get(&ServiceType::KV).unwrap();
        assert!(!memd.is_empty());

        for node in memd {
            assert!(!node.id.is_empty());
            assert!(!node.remote_address.is_empty());
            assert_eq!(ConnectionState::Connected, node.state);
            assert!(node.local_address.as_ref().is_some_and(|la| !la.is_empty()));
            assert!(node.last_activity.is_some_and(|la| la > 0));

            assert_eq!(ServiceType::KV, node.service_type);
        }
    })
}

fn verify_ping_report(report: PingReport, bucket: Option<String>) {
    assert!(report.config_rev > 0);
    assert!(!report.id.is_empty());
    assert_eq!(report.sdk, "rust");
    assert_eq!(report.version, 2);
    assert!(report.services.contains_key(&ServiceType::KV));
    assert!(report.services.contains_key(&ServiceType::QUERY));
    assert!(report.services.contains_key(&ServiceType::SEARCH));

    let memd = report.services.get(&ServiceType::KV).unwrap();
    assert!(!memd.is_empty());

    for node in memd {
        assert!(node.id.is_some());
        assert_eq!(bucket.as_ref(), node.namespace.as_ref());
        assert!(node.error.is_none());
        assert_eq!(PingState::Ok, node.state);
        assert!(!node.latency.is_zero());
        assert!(!node.remote.is_empty());
    }

    let query = report.services.get(&ServiceType::QUERY).unwrap();
    assert!(!query.is_empty());

    for node in query {
        assert!(node.namespace.is_none());
        assert!(node.error.is_none());
        assert_eq!(PingState::Ok, node.state);
        assert!(!node.latency.is_zero());
        assert!(!node.remote.is_empty());
    }

    let search = report.services.get(&ServiceType::SEARCH).unwrap();
    assert!(!search.is_empty());

    for node in search {
        assert!(node.namespace.is_none());
        assert!(node.error.is_none());
        assert_eq!(PingState::Ok, node.state);
        assert!(!node.latency.is_zero());
        assert!(!node.remote.is_empty());
    }
}
