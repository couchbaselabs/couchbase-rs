#![feature(async_closure)]

use crate::common::create_cluster_from_test_config;
use crate::common::doc_generation::{
    import_sample_beer_dataset, TestBreweryDocumentJson, TestMutationResult,
};
use crate::common::test_config::{setup_tests, test_bucket, test_collection, test_scope};
use couchbase::cluster::Cluster;
use couchbase::options::analytics_options::{AnalyticsOptions, ScanConsistency};
use couchbase::results::analytics_results::{AnalyticsMetaData, AnalyticsResult, AnalyticsStatus};
use futures::StreamExt;
use log::LevelFilter;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::time;
use tokio::time::{timeout_at, Instant};

mod common;

#[tokio::test]
async fn test_cluster_analytics_query_basic() {
    run_test("analytics", async |cluster, import_data| {
        let bucket_name = test_bucket().await;
        let scope_name = test_scope().await;
        let collection_name = test_collection().await;

        let query = format!(
            r#"SELECT c.* FROM `{}`.`{}`.`{}` AS c WHERE `service`="analytics""#,
            bucket_name, scope_name, collection_name
        );

        let deadline = Instant::now() + Duration::from_secs(60);

        let (rows, res) =
            run_query_until(deadline, &cluster, import_data.len(), &query, None).await;

        for row in rows {
            import_data.values().find(|doc| doc.doc == row).unwrap();
        }

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
    .await;
}

#[tokio::test]
async fn test_cluster_analytics_query_positional_param() {
    run_test("analytics", async |cluster, import_data| {
        let bucket_name = test_bucket().await;
        let scope_name = test_scope().await;
        let collection_name = test_collection().await;

        let query = format!(
            "SELECT c.* FROM `{}`.`{}`.`{}` AS c WHERE `service`=$1",
            bucket_name, scope_name, collection_name
        );

        let opts = AnalyticsOptions::default()
            .add_positional_parameter("analytics")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(60);

        let (rows, res) =
            run_query_until(deadline, &cluster, import_data.len(), &query, opts).await;

        for row in rows {
            import_data.values().find(|doc| doc.doc == row).unwrap();
        }

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
    .await;
}

#[tokio::test]
async fn test_cluster_analytics_query_named_param() {
    run_test("analytics", async |cluster, import_data| {
        let bucket_name = test_bucket().await;
        let scope_name = test_scope().await;
        let collection_name = test_collection().await;

        let query = format!(
            "SELECT c.* FROM `{}`.`{}`.`{}` AS c WHERE `service`=$service",
            bucket_name, scope_name, collection_name
        );

        let opts = AnalyticsOptions::default()
            .add_named_parameter("service", "analytics")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(60);

        let (rows, res) =
            run_query_until(deadline, &cluster, import_data.len(), &query, opts).await;

        for row in rows {
            import_data.values().find(|doc| doc.doc == row).unwrap();
        }

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
    .await;
}

#[tokio::test]
async fn test_cluster_analytics_query_read_only() {
    run_test("analytics", async |cluster, import_data| {
        let bucket_name = test_bucket().await;
        let scope_name = test_scope().await;
        let collection_name = test_collection().await;

        let query = format!(
            "CREATE PRIMARY INDEX IF NOT EXISTS ON `{}`.`{}`.`{}`",
            bucket_name, scope_name, collection_name
        );

        let opts = AnalyticsOptions::default().read_only(true);

        let mut res = cluster.analytics_query(query, opts).await;

        // TODO: Replace with a more precise error when able to.
        assert!(res.is_err());
    })
    .await;
}

#[tokio::test]
async fn test_cluster_analytics_query_scan_consistency() {
    run_test("analytics_scan_consistency", async |_cluster, import_data| {
        let bucket_name = test_bucket().await;
        let scope_name = test_scope().await;
        let collection_name = test_collection().await;

        let cluster = create_cluster_from_test_config().await;

        let query = format!(
            r#"SELECT c.* FROM `{}`.`{}`.`{}` AS c WHERE `service`="analytics_scan_consistency""#,
            bucket_name, scope_name, collection_name
        );

        let opts = AnalyticsOptions::default().scan_consistency(ScanConsistency::RequestPlus);

        let deadline = Instant::now() + Duration::from_secs(60);

        let mut res = timeout_at(deadline, cluster.analytics_query(query, opts))
            .await
            .unwrap()
            .unwrap();

        while let Some(row) = res.rows().next().await {
            let row = row.unwrap();
            import_data.values().find(|doc| doc.doc == row).unwrap();
        }

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
        .await;
}

fn assert_metadata(meta: AnalyticsMetaData) {
    assert!(!meta.request_id.unwrap().is_empty());
    assert!(!meta.client_context_id.unwrap().is_empty());
    assert_eq!(AnalyticsStatus::Success, meta.status.unwrap());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(5, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!(r#"{"*":"*"}"#, meta.signature.unwrap().get());
}

async fn run_query_until(
    deadline: Instant,
    cluster: &Cluster,
    result_size: usize,
    query: &str,
    opts: impl Into<Option<AnalyticsOptions>>,
) -> (Vec<TestBreweryDocumentJson>, AnalyticsResult) {
    let opts = opts.into();
    loop {
        let mut res = timeout_at(deadline, cluster.analytics_query(query, opts.clone()))
            .await
            .unwrap()
            .unwrap();

        let mut rows = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        if rows.len() == result_size {
            return (rows, res);
        }

        let sleep = time::sleep(Duration::from_secs(1));
        timeout_at(deadline, sleep).await.unwrap();
    }
}

async fn run_test<T, Fut>(data_import_tag: &str, test: T)
where
    T: FnOnce(Cluster, HashMap<String, TestMutationResult>) -> Fut,
    Fut: Future<Output = ()>,
{
    setup_tests(LevelFilter::Trace).await;

    let cluster = create_cluster_from_test_config().await;

    let bucket_name = test_bucket().await;
    let scope_name = test_scope().await;
    let collection_name = test_collection().await;

    enable_analytics_on_collection(&cluster, &bucket_name, &scope_name, &collection_name).await;

    let import = import_sample_beer_dataset(
        data_import_tag,
        &cluster
            .bucket(&bucket_name)
            .scope(&scope_name)
            .collection(&collection_name),
    )
    .await;

    test(cluster, import).await
}

async fn enable_analytics_on_collection(
    cluster: &Cluster,
    bucket_name: impl Into<String>,
    scope_name: impl Into<String>,
    collection_name: impl Into<String>,
) {
    let bucket_name = bucket_name.into();
    let scope_name = scope_name.into();
    let collection_name = collection_name.into();

    cluster
        .analytics_query(
            format!(
                "CREATE ANALYTICS SCOPE `{}`.`{}` IF NOT EXISTS",
                &bucket_name, &scope_name,
            ),
            None,
        )
        .await
        .unwrap();

    cluster
        .analytics_query(
            format!(
                "CREATE ANALYTICS COLLECTION IF NOT EXISTS {}.{}.{} ON `{}`.`{}`.`{}`",
                &bucket_name,
                &scope_name,
                &collection_name,
                &bucket_name,
                &scope_name,
                &collection_name,
            ),
            None,
        )
        .await
        .unwrap();
}
