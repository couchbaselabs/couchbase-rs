use crate::common::consistency_utils::{
    verify_collection_created, verify_scope_created, verify_scope_dropped,
};
use crate::common::test_config::run_test;
use crate::common::try_until;
use couchbase::management::collections::collection_manager::CollectionManager;
use couchbase::options::query_index_mgmt_options::{
    CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
};
use couchbase::options::query_options::QueryOptions;
use couchbase::results::query_results::{QueryMetaData, QueryStatus};
use futures::StreamExt;
use serde_json::value::RawValue;
use serde_json::Value;
use std::time::Duration;

mod common;

#[test]
fn test_query_basic() {
    run_test(async |cluster| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_query_empty_result() {
    run_test(async |cluster| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster
            .query("SELECT * FROM ARRAY_RANGE(0, 0) AS x", opts)
            .await
            .unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(0, rows.len());
    })
}

#[test]
fn test_query_error() {
    run_test(async |cluster| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster.query("SELEC 1=1", opts).await;

        // TODO: once error handling is improved, this should be a proper error.
        assert!(res.is_err());
        assert!(res
            .err()
            .unwrap()
            .msg
            .contains("parsing failure code: 3000"));
    })
}

#[test]
fn test_query_raw_result() {
    run_test(async |cluster| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Box<RawValue>> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_str(row.get()).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_prepared_query_basic() {
    run_test(async |cluster| {
        let opts = QueryOptions::new().metrics(true);
        let mut res = cluster.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_scope_query_basic() {
    run_test(async |cluster| {
        let scope = cluster
            .bucket(&cluster.default_bucket)
            .scope(&cluster.default_scope);

        let opts = QueryOptions::new().metrics(true);
        let mut res = scope.query("SELECT 1=1", opts).await.unwrap();

        let mut rows: Vec<Value> = vec![];
        while let Some(row) = res.rows().next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_obj = row.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().await.unwrap();
        assert_metadata(meta);
    })
}

#[test]
fn test_query_indexes() {
    run_test(async |cluster| {
        let coll_manager = cluster.bucket(&cluster.default_bucket).collections();
        let (scope, collection) = create_collection(&coll_manager).await;

        let manager = cluster
            .bucket(&cluster.default_bucket)
            .scope(&scope)
            .collection(&collection)
            .query_indexes();

        let opts = CreatePrimaryQueryIndexOptions::new().ignore_if_exists(true);

        // Allow time for server to sync with the new collection
        try_until(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_millis(100),
            "Primary index was not created in time",
            async || {
                let res = manager.create_primary_index(opts.clone()).await;
                if res.is_ok() {
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            },
        )
        .await;

        let opts = CreateQueryIndexOptions::new()
            .ignore_if_exists(true)
            .deferred(true);
        manager
            .create_index("test_index".to_string(), vec!["name".to_string()], opts)
            .await
            .unwrap();

        let indexes = manager.get_all_indexes(None).await.unwrap();

        assert_eq!(2, indexes.len());

        let primary_index = indexes.iter().find(|idx| idx.name() == "#primary").unwrap();
        assert!(primary_index.is_primary());
        assert_eq!(primary_index.state(), "Online");

        let test_index = indexes
            .iter()
            .find(|idx| idx.name() == "test_index")
            .unwrap();
        assert!(!test_index.is_primary());
        assert_eq!(test_index.state(), "Deferred");
        assert_eq!(test_index.keyspace(), &collection);

        manager.build_deferred_indexes(None).await.unwrap();

        manager
            .watch_indexes(vec!["test_index".to_string()], None)
            .await
            .unwrap();

        manager.drop_primary_index(None).await.unwrap();

        manager
            .drop_index("test_index".to_string(), None)
            .await
            .unwrap();

        let indexes = manager.get_all_indexes(None).await.unwrap();
        assert_eq!(0, indexes.len());

        drop_scope(&coll_manager, &scope).await;
    })
}

async fn create_collection(manager: &CollectionManager) -> (String, String) {
    let scope_name = common::generate_string_value(10);
    let collection_name = common::generate_string_value(10);

    manager.create_scope(&scope_name, None).await.unwrap();
    verify_scope_created(manager, &scope_name).await;

    let settings =
        couchbase::management::collections::collection_manager::CreateCollectionSettings::new();
    manager
        .create_collection(&scope_name, &collection_name, settings, None)
        .await
        .unwrap();

    verify_collection_created(manager, &scope_name, &collection_name).await;

    (scope_name, collection_name)
}

async fn drop_scope(manager: &CollectionManager, scope_name: &str) {
    manager.drop_scope(scope_name, None).await.unwrap();
    verify_scope_dropped(manager, scope_name).await;
}

fn assert_metadata(meta: QueryMetaData) {
    assert!(!meta.request_id.is_empty());
    assert!(!meta.client_context_id.is_empty());
    assert_eq!(QueryStatus::Success, meta.status);
    assert!(meta.profile.is_none());
    assert!(meta.warnings.is_empty());

    assert!(!meta.metrics.elapsed_time.is_zero());
    assert!(!meta.metrics.execution_time.is_zero());
    assert_eq!(1, meta.metrics.result_count);
    assert_ne!(0, meta.metrics.result_size);
    assert_eq!(0, meta.metrics.mutation_count);
    assert_eq!(0, meta.metrics.sort_count);
    assert_eq!(0, meta.metrics.error_count);
    assert_eq!(0, meta.metrics.warning_count);

    assert_eq!(
        "{\"$1\":\"boolean\"}",
        meta.signature.as_ref().unwrap().get()
    );
}
