use crate::common::default_agent_options::create_default_options;
use crate::common::helpers::{generate_bytes_value, generate_key};
use crate::common::test_config::{setup_test, take_captured_logs, with_log_capture};
use couchbase_core::agent::Agent;
use couchbase_core::options::agent::OrphanReporterConfig;
use couchbase_core::options::crud::UpsertOptions;
use couchbase_core::orphan_reporter::OrphanReporter;
use couchbase_core::retryfailfast::FailFastRetryStrategy;
use std::sync::Arc;
use std::time::Duration;

mod common;

#[test]
fn test_orphan_reporter_logs() {
    setup_test(async |config| {
        let _guard = with_log_capture();

        let orphan_reporter = OrphanReporter::new(
            OrphanReporterConfig::default().reporter_interval(Duration::from_secs(1)),
        );
        let agent_opts = create_default_options(config)
            .await
            .orphan_reporter_handler(orphan_reporter.get_handle());

        let agent = Agent::new(agent_opts).await.unwrap();

        let strat = Arc::new(FailFastRetryStrategy::default());
        let value = generate_bytes_value(32);

        // Let agent bootstrap
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Run ops until we see a timeout (which should be an orphan)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut got_timeout = false;
        while tokio::time::Instant::now() < deadline && !got_timeout {
            for _ in 0..10u8 {
                let key = generate_key();
                let res = tokio::time::timeout(
                    Duration::from_millis(1),
                    agent.upsert(
                        UpsertOptions::new(key.as_slice(), "", "", value.as_slice())
                            .retry_strategy(strat.clone()),
                    ),
                )
                .await;
                if res.is_err() {
                    got_timeout = true;
                }
            }
            if !got_timeout {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert!(
            got_timeout,
            "expected at least one timeout to generate orphans"
        );

        // Allow reporter to flush
        tokio::time::sleep(Duration::from_secs(2)).await;

        let out = take_captured_logs();
        let prefix = "Orphaned responses observed: ";
        let json_line_opt = out.lines().find(|l| l.contains(prefix));
        assert!(
            json_line_opt.is_some(),
            "expected orphan reporter log line, got:\n{}",
            out
        );
        let json_line = json_line_opt.unwrap();

        let json_str_opt = json_line.split_once(prefix).map(|x| x.1);
        assert!(
            json_str_opt.is_some(),
            "expected JSON payload after prefix in log line: {}",
            json_line
        );
        let json_str = json_str_opt.unwrap();

        // Try top-level service map first: {"kv": {"total_count":..,"top_requests":[..]}}
        let v_res: Result<serde_json::Value, _> = serde_json::from_str(json_str);
        assert!(v_res.is_ok(), "valid JSON expected, got: {}", json_str);
        let v = v_res.unwrap();

        let is_entry_level = v.get("top_requests").is_some();
        let (entry, service_key) = if is_entry_level {
            (v, None::<&String>)
        } else {
            let obj = v.as_object();
            assert!(obj.is_some(), "service map object expected: {}", json_str);
            let obj = obj.unwrap();
            let kv_or_first = obj.get_key_value("kv").or_else(|| obj.iter().next());
            assert!(
                kv_or_first.is_some(),
                "service map non-empty expected: {}",
                json_str
            );
            let (k, e) = kv_or_first.unwrap();
            (e.clone(), Some(k))
        };

        // Validate entry shape
        let total_count_opt = entry.get("total_count").and_then(|x| x.as_u64());
        assert!(total_count_opt.is_some(), "total_count is missing");
        let total_count = total_count_opt.unwrap();
        assert!(total_count >= 1, "total_count should be >= 1");

        let top_opt = entry.get("top_requests").and_then(|x| x.as_array());
        assert!(top_opt.is_some(), "top_requests is missing");
        let top = top_opt.unwrap();
        assert!(!top.is_empty(), "top_requests is empty");

        // Validate sorting: total_server_duration_us must be non-increasing
        let mut prev: Option<u64> = None;
        for (idx, it) in top.iter().enumerate() {
            let v_opt = it.get("total_server_duration_us").and_then(|x| x.as_u64());
            assert!(
                v_opt.is_some(),
                "top[{}].total_server_duration_us missing/u64",
                idx
            );
            let v = v_opt.unwrap();
            if let Some(p) = prev {
                assert!(
                    p >= v,
                    "items not sorted descending by total_server_duration_us: {} < {} at index {}",
                    p,
                    v,
                    idx
                );
            }
            prev = Some(v);
        }

        // Check all fields are present
        let item = &top[0];
        for key in ["last_server_duration_us", "total_server_duration_us"] {
            let n = item.get(key).and_then(|x| x.as_u64());
            assert!(n.is_some(), "{} missing/u64", key);
        }
        for key in [
            "operation_name",
            "last_local_id",
            "operation_id",
            "last_local_socket",
            "last_remote_socket",
        ] {
            let s = item.get(key).and_then(|x| x.as_str());
            assert!(s.is_some(), "{} missing/str", key);
            assert!(!s.unwrap().is_empty(), "{} must be non-empty", key);
        }

        if let Some(svc) = service_key {
            assert_eq!(svc, "kv", "expected kv service key");
        }

        assert!(
            out.contains("Orphaned responses observed:"),
            "expected orphan reporter output, got:\n{}",
            out
        );
    });
}
