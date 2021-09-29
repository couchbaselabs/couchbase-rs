use crate::util::{try_until, upsert_brewery_dataset, BreweryDocument, TestConfig, TestFeature};
use couchbase::{
    Bucket, Cluster, Collection, CreatePrimaryQueryIndexOptions, QueryMetaData, QueryOptions,
    QueryProfile, QueryStatus,
};
use futures::StreamExt;

use crate::{TestError, TestResult};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn verify_query_result(meta: QueryMetaData, expected_docs: Vec<BreweryDocument>) {
    let metrics = meta.metrics().unwrap();
    assert_eq!(0, metrics.error_count());
    assert!(!metrics.elapsed_time().is_zero());
    assert!(!metrics.execution_time().is_zero());
    assert!(metrics.result_size() > 0);
    assert_eq!(expected_docs.len(), metrics.result_count());
    assert!(!meta.request_id().is_empty());
    assert_eq!(QueryStatus::Success, meta.status());
    assert!(meta.warnings().is_none());
    assert!(!meta.client_context_id().is_empty());
    assert!(meta.signature::<Value>().is_some());
    assert!(meta.profile::<Value>().is_some());
}

async fn setup_query_test(
    test_name: &str,
    cluster: &Cluster,
    bucket: &Bucket,
    collection: &Collection,
) -> TestResult<Vec<BreweryDocument>> {
    let expected_docs = upsert_brewery_dataset(test_name, collection).await?;

    let idx_mgr = cluster.query_indexes();
    idx_mgr
        .create_primary_index(
            bucket.name(),
            CreatePrimaryQueryIndexOptions::default().ignore_if_exists(true),
        )
        .await?;

    Ok(expected_docs)
}

pub async fn test_query(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    let cluster = config.cluster();
    let expected_docs =
        setup_query_test("test_query", cluster, config.bucket(), config.collection()).await?;

    let result = try_until(Instant::now().add(Duration::from_secs(10)), || async {
        let mut result = cluster
            .query(
                format!(
                    "SELECT {}.* FROM {} WHERE `test` = \"{}\"",
                    config.bucket().name(),
                    config.bucket().name(),
                    "test_query"
                ),
                QueryOptions::default().profile(QueryProfile::Timings),
            )
            .await?;

        let mut docs: Vec<BreweryDocument> = vec![];
        let mut rows = result.rows::<BreweryDocument>();
        while let Some(row) = rows.next().await {
            docs.push(row?);
        }
        let meta = result.meta_data().await?;

        if docs.len() == expected_docs.len() {
            Ok::<(Vec<BreweryDocument>, QueryMetaData), TestError>((docs, meta))
        } else {
            Err::<(Vec<BreweryDocument>, QueryMetaData), TestError>(TestError {
                reason: format!("Expected 5 rows but got {}", docs.len()),
            })
        }
    })
    .await?;

    verify_query_result(result.1, expected_docs);

    Ok(false)
}

pub async fn test_query_named_params(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    let cluster = config.cluster();
    let expected_docs = setup_query_test(
        "test_query_named_params",
        cluster,
        config.bucket(),
        config.collection(),
    )
    .await?;

    let result = try_until(Instant::now().add(Duration::from_secs(10)), || async {
        let mut params = HashMap::new();
        params.insert("testname", "test_query_named_params");
        let mut result = cluster
            .query(
                format!(
                    "SELECT {}.* FROM {} WHERE `test` = $testname",
                    config.bucket().name(),
                    config.bucket().name(),
                ),
                QueryOptions::default()
                    .profile(QueryProfile::Timings)
                    .named_parameters(params),
            )
            .await?;

        let mut docs: Vec<BreweryDocument> = vec![];
        let mut rows = result.rows::<BreweryDocument>();
        while let Some(row) = rows.next().await {
            docs.push(row?);
        }
        let meta = result.meta_data().await?;

        if docs.len() == expected_docs.len() {
            Ok::<(Vec<BreweryDocument>, QueryMetaData), TestError>((docs, meta))
        } else {
            Err::<(Vec<BreweryDocument>, QueryMetaData), TestError>(TestError {
                reason: format!("Expected 5 rows but got {}", docs.len()),
            })
        }
    })
    .await?;

    verify_query_result(result.1, expected_docs);

    Ok(false)
}

pub async fn test_query_positional_params(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    let cluster = config.cluster();
    let expected_docs = setup_query_test(
        "test_query_positional_params",
        cluster,
        config.bucket(),
        config.collection(),
    )
    .await?;

    let result = try_until(Instant::now().add(Duration::from_secs(10)), || async {
        let params = vec!["test_query_positional_params"];
        let mut result = cluster
            .query(
                format!(
                    "SELECT {}.* FROM {} WHERE `test` = $1",
                    config.bucket().name(),
                    config.bucket().name(),
                ),
                QueryOptions::default()
                    .profile(QueryProfile::Timings)
                    .positional_parameters(params),
            )
            .await?;

        let mut docs: Vec<BreweryDocument> = vec![];
        let mut rows = result.rows::<BreweryDocument>();
        while let Some(row) = rows.next().await {
            docs.push(row?);
        }
        let meta = result.meta_data().await?;

        if docs.len() == expected_docs.len() {
            Ok::<(Vec<BreweryDocument>, QueryMetaData), TestError>((docs, meta))
        } else {
            Err::<(Vec<BreweryDocument>, QueryMetaData), TestError>(TestError {
                reason: format!("Expected 5 rows but got {}", docs.len()),
            })
        }
    })
    .await?;

    verify_query_result(result.1, expected_docs);

    Ok(false)
}

pub async fn test_query_prepared(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    let cluster = config.cluster();
    let expected_docs = setup_query_test(
        "test_query_prepared",
        cluster,
        config.bucket(),
        config.collection(),
    )
    .await?;

    let result = try_until(Instant::now().add(Duration::from_secs(10)), || async {
        let mut result = cluster
            .query(
                format!(
                    "SELECT {}.* FROM {} WHERE `test` = \"{}\"",
                    config.bucket().name(),
                    config.bucket().name(),
                    "test_query_prepared"
                ),
                QueryOptions::default()
                    .profile(QueryProfile::Timings)
                    .adhoc(false),
            )
            .await?;

        let mut docs: Vec<BreweryDocument> = vec![];
        let mut rows = result.rows::<BreweryDocument>();
        while let Some(row) = rows.next().await {
            docs.push(row?);
        }
        let meta = result.meta_data().await?;

        if docs.len() == expected_docs.len() {
            Ok::<(Vec<BreweryDocument>, QueryMetaData), TestError>((docs, meta))
        } else {
            Err::<(Vec<BreweryDocument>, QueryMetaData), TestError>(TestError {
                reason: format!("Expected 5 rows but got {}", docs.len()),
            })
        }
    })
    .await?;

    verify_query_result(result.1, expected_docs);

    Ok(false)
}
