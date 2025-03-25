extern crate core;

use futures::StreamExt;
use serde_json::Value;

use couchbase_core::queryoptions::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, GetAllIndexesOptions, QueryOptions, WatchIndexesOptions,
};
use couchbase_core::queryx::query_result::Status;

use crate::common::test_config::run_test;

mod common;

#[test]
fn test_query_basic() {
    run_test(async |mut agent| {
        let opts = QueryOptions::default().statement("SELECT 1=1".to_string());
        let mut res = agent.query(opts).await.unwrap();

        let mut rows = vec![];
        while let Some(row) = res.next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_slice(row).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert!(meta.prepared.is_none());
        assert!(!meta.request_id.is_empty());
        assert!(!meta.client_context_id.is_empty());
        assert_eq!(Status::Success, meta.status);
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

        dbg!(&meta.signature);

        assert_eq!(
            "{\"$1\":\"boolean\"}",
            meta.signature.as_ref().unwrap().get()
        );
    });
}

#[test]
fn test_prepared_query_basic() {
    run_test(async |mut agent| {
        let opts = QueryOptions::default().statement("SELECT 1=1".to_string());
        let mut res = agent.prepared_query(opts).await.unwrap();

        let mut rows = vec![];
        while let Some(row) = res.next().await {
            rows.push(row.unwrap());
        }

        assert_eq!(1, rows.len());

        let row = rows.first().unwrap();

        let row_value: Value = serde_json::from_slice(row).unwrap();
        let row_obj = row_value.as_object().unwrap();

        assert!(row_obj.get("$1").unwrap().as_bool().unwrap());

        let meta = res.metadata().unwrap();
        assert!(meta.prepared.is_some());
        assert!(!meta.request_id.is_empty());
        assert!(!meta.client_context_id.is_empty());
        assert_eq!(Status::Success, meta.status);
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

        dbg!(&meta.signature);

        // MB-65750: signature is inconsistent in format.
        assert!(!meta.signature.as_ref().unwrap().get().is_empty());
    });
}

#[test]
fn test_query_indexes() {
    run_test(async |mut agent| {
        let bucket_name = agent.test_setup_config.bucket.clone();

        let opts = CreatePrimaryIndexOptions::new(bucket_name.as_str()).ignore_if_exists(true);

        agent.create_primary_index(&opts).await.unwrap();

        let opts = CreateIndexOptions::new(bucket_name.as_str(), "test_index", &["name"])
            .ignore_if_exists(true)
            .deferred(true);

        agent.create_index(&opts).await.unwrap();

        let opts = GetAllIndexesOptions::new(bucket_name.as_str());

        let indexes = agent.get_all_indexes(&opts).await.unwrap();
        assert_eq!(2, indexes.len());

        let opts = BuildDeferredIndexesOptions::new(bucket_name.as_str());

        agent.build_deferred_indexes(&opts).await.unwrap();

        let opts = WatchIndexesOptions::new(bucket_name.as_str(), &["test_index"]);

        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            agent.watch_indexes(&opts),
        )
        .await
        .unwrap()
        .unwrap();

        let opts = DropPrimaryIndexOptions::new(bucket_name.as_str()).ignore_if_not_exists(true);

        agent.drop_primary_index(&opts).await.unwrap();

        let opts =
            DropIndexOptions::new(bucket_name.as_str(), "test_index").ignore_if_not_exists(true);

        agent.drop_index(&opts).await.unwrap();
        let opts = GetAllIndexesOptions::new(bucket_name.as_str());

        let indexes = agent.get_all_indexes(&opts).await.unwrap();

        assert_eq!(0, indexes.len());
    });
}
