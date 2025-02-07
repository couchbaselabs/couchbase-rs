use crate::common::new_key;
use crate::common::test_config::run_test;
use crate::span_assertion::{
    assign_collection_level_span_fields, assign_common_fields, assign_query_span_fields,
    assign_search_span_fields, create_common_dispatch_span_fields, create_encoding_span_assertion,
    create_kv_dispatch_span_assertion, create_query_dispatch_span_assertion, finalize_assertion,
    TestLogWriter,
};
use couchbase::collections_manager::CreateCollectionSettings;
use couchbase::search::queries::{Query, TermQuery};
use couchbase::search::request::SearchRequest;
use couchbase::threshold_log_tracer::{ThresholdLoggingLayer, ThresholdLoggingOptions};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{info_span, span, Level};
use tracing_fluent_assertions::{AssertionRegistry, AssertionsLayer};
use tracing_subscriber::fmt::TestWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, Registry};

mod common;

#[test]
fn test_tracing_kv() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(&cluster.default_bucket)
            .scope(&cluster.default_scope)
            .collection(&cluster.default_collection);

        let key = new_key();

        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion = assign_collection_level_span_fields(
            &cluster,
            assertion_registry.build().with_name("upsert"),
        );
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let encoding_span_assertion = create_encoding_span_assertion(&cluster, &assertion_registry)
            .with_parent_name("upsert");
        let encoding_span_assertion = finalize_assertion(encoding_span_assertion);

        let dispatch_span_assertion =
            create_kv_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("upsert");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        // We don't care about the result, just that the spans are correct, so no need to unwrap
        let _ = collection.upsert(&key, "test", None).await;

        // Sleep to ensure subscriber processes spans closing
        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        encoding_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_tracing_query() {
    run_test(async |cluster| {
        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion =
            assign_query_span_fields(&cluster, assertion_registry.build().with_name("query"))
                .with_span_field("db.name")
                .with_span_field("db.couchbase.scope");
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let dispatch_span_assertion =
            create_query_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("query");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        let _ = cluster
            .bucket(&cluster.default_bucket)
            .scope(&cluster.default_scope)
            .query("select 1=1", None)
            .await;

        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_tracing_analytics() {
    run_test(async |cluster| {
        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion =
            assign_query_span_fields(&cluster, assertion_registry.build().with_name("analytics"));
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let dispatch_span_assertion =
            create_query_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("analytics");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        let _ = cluster
            .analytics_query("SELECT \"Hello, data!\" AS greeting;", None)
            .await;

        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_tracing_search() {
    run_test(async |cluster| {
        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion =
            assign_search_span_fields(&cluster, assertion_registry.build().with_name("search"));
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let dispatch_span_assertion =
            create_common_dispatch_span_fields(&cluster, &assertion_registry)
                .with_parent_name("search");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);
        let query = TermQuery::new("search").field("service".to_string());

        let basic_request = SearchRequest::with_search_query(Query::Term(query.clone()));

        let _ = cluster
            .search("basic_search_index", basic_request, None)
            .await;

        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_tracing_data_structures() {
    run_test(async |cluster| {
        let collection = cluster
            .bucket(&cluster.default_bucket)
            .scope(&cluster.default_scope)
            .collection(&cluster.default_collection);
        let key = new_key();

        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion = assign_collection_level_span_fields(
            &cluster,
            assertion_registry.build().with_name("list_append"),
        );
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let inner_span_assertion = assign_collection_level_span_fields(
            &cluster,
            assertion_registry.build().with_name("mutate_in"),
        )
        .with_parent_name("list_append");
        let inner_span_assertion = finalize_assertion(inner_span_assertion);

        let dispatch_span_assertion =
            create_kv_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("mutate_in");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        let list = collection.list(&key, None);
        list.append("test2").await.unwrap();

        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        inner_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_query_management() {
    run_test(async |cluster| {
        let manager = cluster
            .bucket(&cluster.default_bucket)
            .scope(&cluster.default_scope)
            .collection("does-not-exist")
            .query_indexes();

        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion = assign_collection_level_span_fields(
            &cluster,
            assertion_registry
                .build()
                .with_name("manager_query_create_index"),
        );
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let query_span_assertion = assertion_registry
            .build()
            .with_name("query")
            .with_span_field("db.statement");
        let query_span_assertion = assign_common_fields(&cluster, query_span_assertion)
            .with_parent_name("manager_query_create_index");

        let dispatch_span_assertion =
            create_query_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("query");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        let query_span_assertion = finalize_assertion(query_span_assertion);

        let _ = manager
            .create_index("test_index".to_string(), vec!["name".to_string()], None)
            .await;
        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        query_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[test]
fn test_collections_management() {
    run_test(async |cluster| {
        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let assertion_registry = AssertionRegistry::default();
        let subscriber = Registry::default().with(AssertionsLayer::new(&assertion_registry));

        let _guard = tracing::subscriber::set_default(subscriber);

        let outer_span_assertion = assign_collection_level_span_fields(
            &cluster,
            assertion_registry
                .build()
                .with_name("manager_collections_create_collection"),
        );
        let outer_span_assertion = finalize_assertion(outer_span_assertion);

        let dispatch_span_assertion =
            create_query_dispatch_span_assertion(&cluster, &assertion_registry)
                .with_parent_name("manager_collections_create_collection");
        let dispatch_span_assertion = finalize_assertion(dispatch_span_assertion);

        let _ = manager
            .create_collection(
                "test_scope",
                "test_collection",
                CreateCollectionSettings::new(),
                None,
            )
            .await;

        sleep(Duration::from_secs(2)).await;

        outer_span_assertion.assert();
        dispatch_span_assertion.assert();
    })
}

#[tokio::test]
async fn test_threshold_logging_layer() {
    let log_buffer = Arc::new(Mutex::new(Vec::new()));
    let writer = TestLogWriter {
        buffer: log_buffer.clone(),
    };

    let threshold_logger = ThresholdLoggingLayer::new(Some(
        ThresholdLoggingOptions::new()
            .kv_threshold(Duration::from_millis(1))
            .query_threshold(Duration::from_millis(1))
            .analytics_threshold(Duration::from_millis(1))
            .search_threshold(Duration::from_millis(1))
            .analytics_threshold(Duration::from_millis(1))
            .management_threshold(Duration::from_millis(1))
            .emit_interval(Duration::from_secs(1)),
    ));

    let subscriber = Registry::default()
        .with(threshold_logger)
        .with(tracing_subscriber::fmt::layer().with_writer(writer));

    let _guard = tracing::subscriber::set_default(subscriber);

    let span = info_span!(target: "couchbase::", "kv_operation", db.couchbase.service = "kv");
    sleep(Duration::from_millis(100)).await;
    drop(span);

    let span = info_span!(target: "couchbase::", "query_operation", db.couchbase.service = "query");
    sleep(Duration::from_millis(100)).await;
    drop(span);

    let span =
        info_span!(target: "couchbase::", "search_operation", db.couchbase.service = "search");
    sleep(Duration::from_millis(100)).await;
    drop(span);

    let span = info_span!(target: "couchbase::", "analytics_operation", db.couchbase.service = "analytics");
    sleep(Duration::from_millis(100)).await;
    drop(span);

    let span = info_span!(target: "couchbase::", "management_operation", db.couchbase.service = "management");
    sleep(Duration::from_millis(100)).await;
    drop(span);

    let timeout_result = timeout(Duration::from_secs(5), async {
        loop {
            {
                let logs = log_buffer.lock().unwrap();
                if !logs.is_empty() {
                    break;
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await;
    assert!(timeout_result.is_ok(), "Timed out waiting for logs");

    let binding = log_buffer.lock().unwrap();

    let log_output = String::from_utf8_lossy(&binding);

    let json_start = log_output.find('{').unwrap();
    let json_str = &log_output[json_start..];

    let parsed_logs: serde_json::Value = serde_json::from_str(json_str).unwrap();

    let expected_services = ["kv", "query", "search", "analytics", "management"];
    for service in &expected_services {
        assert!(
            parsed_logs.get(service).is_some(),
            "Missing service in logs: {}",
            service
        );
    }

    for service in &expected_services {
        let service_logs = parsed_logs.get(service).unwrap();
        assert!(service_logs.get("total_count").is_some());
        assert!(service_logs.get("top_requests").is_some());

        let top_requests = service_logs
            .get("top_requests")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(top_requests.len(), 1);

        let request = &top_requests[0];
        assert!(request.get("operation_name").is_some());
        assert!(request.get("total_duration_us").is_some());
    }
}

mod span_assertion {
    use crate::common::features::TestFeatureCode;
    use crate::common::test_config::TestCluster;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing_fluent_assertions::assertion::NoCriteria;
    use tracing_fluent_assertions::{Assertion, AssertionBuilder, AssertionRegistry};
    use tracing_subscriber::fmt::MakeWriter;

    pub(crate) fn assign_collection_level_span_fields(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = assign_common_outer_span_fields(cluster, builder);

        builder
            .with_span_field("db.name")
            .with_span_field("db.couchbase.scope")
            .with_span_field("db.couchbase.collection")
            .with_span_field("db.operation")
    }

    pub(crate) fn assign_query_span_fields(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = assign_common_outer_span_fields(cluster, builder);

        builder.with_span_field("db.statement")
    }

    pub(crate) fn assign_search_span_fields(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = assign_common_outer_span_fields(cluster, builder);

        builder.with_span_field("db.operation")
    }

    pub(crate) fn create_encoding_span_assertion(
        cluster: &TestCluster,
        assertion_registry: &AssertionRegistry,
    ) -> AssertionBuilder<NoCriteria> {
        assign_common_fields(
            cluster,
            assertion_registry.build().with_name("request_encoding"),
        )
    }

    pub(crate) fn create_kv_dispatch_span_assertion(
        cluster: &TestCluster,
        assertion_registry: &AssertionRegistry,
    ) -> AssertionBuilder<NoCriteria> {
        create_common_dispatch_span_fields(cluster, assertion_registry)
            .with_name("dispatch_to_server")
            .with_span_field("db.couchbase.server_duration")
            .with_span_field("db.couchbase.local_id")
            .with_span_field("net.host.name")
            .with_span_field("net.host.port")
            .with_span_field("db.couchbase.operation_id")
    }

    pub(crate) fn create_query_dispatch_span_assertion(
        cluster: &TestCluster,
        assertion_registry: &AssertionRegistry,
    ) -> AssertionBuilder<NoCriteria> {
        create_common_dispatch_span_fields(cluster, assertion_registry)
            .with_span_field("db.couchbase.operation_id")
    }

    pub(crate) fn create_common_dispatch_span_fields(
        cluster: &TestCluster,
        assertion_registry: &AssertionRegistry,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = assign_common_fields(
            cluster,
            assertion_registry.build().with_name("dispatch_to_server"),
        );

        builder
            .with_span_field("net.transport")
            .with_span_field("net.peer.name")
            .with_span_field("net.peer.port")
    }

    pub(crate) fn finalize_assertion(builder: AssertionBuilder<NoCriteria>) -> Assertion {
        builder
            .was_created()
            .was_entered()
            .was_exited()
            .was_closed()
            .finalize()
    }

    pub(crate) fn assign_common_fields(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = builder.with_span_field("db.system");
        assign_cluster_labels(cluster, builder)
    }

    fn assign_cluster_labels(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        if cluster.supports_feature(&TestFeatureCode::ClusterLabels) {
            builder
                .with_span_field("db.couchbase.cluster_name")
                .with_span_field("db.couchbase.cluster_uuid")
        } else {
            builder
        }
    }

    fn assign_common_outer_span_fields(
        cluster: &TestCluster,
        builder: AssertionBuilder<NoCriteria>,
    ) -> AssertionBuilder<NoCriteria> {
        let builder = assign_common_fields(cluster, builder);

        builder
            .with_span_field("db.couchbase.service")
            .with_span_field("db.couchbase.retries")
    }

    pub(crate) struct TestLogWriter {
        pub(crate) buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl MakeWriter<'_> for TestLogWriter {
        type Writer = TestWriter;

        fn make_writer(&self) -> Self::Writer {
            TestWriter {
                buffer: self.buffer.clone(),
            }
        }
    }

    pub(crate) struct TestWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
