use crate::common::features::TestFeatureCode;
use crate::common::helpers::{
    create_collection_and_ensure_exists, create_scope_and_ensure_exists, delete_scope,
    generate_key_with_letter_prefix, generate_string_key, try_until,
};
use crate::common::test_config::run_test;
use couchbase_core::searchmgmt_options::{
    DeleteIndexOptions, EnsureIndexOptions, GetIndexOptions, UpsertIndexOptions,
};
use couchbase_core::searchx::ensure_index_helper::DesiredState;
use couchbase_core::searchx::index::Index;
use std::future::Future;
use std::ops::Add;
use std::time::Duration;
use tokio::time::Instant;

mod common;

#[test]
fn test_basic_search_index_management() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::SearchManagement) {
            return;
        }

        let index_name = generate_key_with_letter_prefix();

        let index = load_search_index(&index_name, &agent.test_setup_config.bucket).await;

        agent
            .upsert_search_index(&UpsertIndexOptions::new(&index))
            .await
            .unwrap();

        agent
            .ensure_search_index(&EnsureIndexOptions::new(
                &index_name,
                None,
                None,
                DesiredState::Created,
                None,
            ))
            .await
            .unwrap();

        let actual_index = agent
            .get_search_index(&GetIndexOptions::new(&index_name))
            .await
            .unwrap();

        assert_eq!(index.name, actual_index.name);
        assert_eq!(index.index_type, actual_index.index_type);
        assert_eq!(index.params, actual_index.params);
        assert_eq!(index.plan_params, actual_index.plan_params);
        assert_eq!(index.prev_index_uuid, actual_index.prev_index_uuid);
        assert_eq!(index.source_name, actual_index.source_name);
        assert_eq!(index.source_params, actual_index.source_params);
        assert_eq!(index.source_type, actual_index.source_type);

        agent
            .delete_search_index(&DeleteIndexOptions::new(&index_name))
            .await
            .unwrap();

        agent
            .ensure_search_index(&EnsureIndexOptions::new(
                &index_name,
                None,
                None,
                DesiredState::Deleted,
                None,
            ))
            .await
            .unwrap();
    });
}

#[test]
fn test_basic_search_index_management_collections() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::SearchManagement)
            || !agent.supports_feature(&TestFeatureCode::SearchManagementCollections)
        {
            return;
        }

        let index_name = generate_key_with_letter_prefix();
        let scope_name = generate_string_key();
        let collection_name = generate_string_key();

        let index = load_scoped_search_index(
            &index_name,
            &agent.test_setup_config.bucket,
            &scope_name,
            &collection_name,
        )
        .await;

        create_scope_and_ensure_exists(&agent, &agent.test_setup_config.bucket, &scope_name).await;
        create_collection_and_ensure_exists(
            &agent,
            &agent.test_setup_config.bucket,
            &scope_name,
            &collection_name,
        )
        .await;

        // fts can return us a scope not found error even after ns_server has acknowledged it exists.
        try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(500),
            "upsert didn't succeed in time",
            async || match agent
                .upsert_search_index(
                    &UpsertIndexOptions::new(&index)
                        .bucket_name(&agent.test_setup_config.bucket)
                        .scope_name(&scope_name),
                )
                .await
            {
                Ok(_) => Ok(Some(())),
                Err(e) => Err(e),
            },
        )
        .await;

        agent
            .ensure_search_index(&EnsureIndexOptions::new(
                &index_name,
                agent.test_setup_config.bucket.as_str(),
                scope_name.as_str(),
                DesiredState::Created,
                None,
            ))
            .await
            .unwrap();

        let actual_index = agent
            .get_search_index(
                &GetIndexOptions::new(&index_name)
                    .bucket_name(&agent.test_setup_config.bucket)
                    .scope_name(&scope_name),
            )
            .await
            .unwrap();

        assert_eq!(index.name, actual_index.name);
        assert_eq!(index.index_type, actual_index.index_type);
        assert_eq!(index.params, actual_index.params);
        assert_eq!(index.plan_params, actual_index.plan_params);
        assert_eq!(index.prev_index_uuid, actual_index.prev_index_uuid);
        assert_eq!(index.source_name, actual_index.source_name);
        assert_eq!(index.source_params, actual_index.source_params);
        assert_eq!(index.source_type, actual_index.source_type);

        agent
            .delete_search_index(
                &DeleteIndexOptions::new(&index_name)
                    .bucket_name(&agent.test_setup_config.bucket)
                    .scope_name(&scope_name),
            )
            .await
            .unwrap();

        agent
            .ensure_search_index(&EnsureIndexOptions::new(
                &index_name,
                agent.test_setup_config.bucket.as_str(),
                scope_name.as_str(),
                DesiredState::Deleted,
                None,
            ))
            .await
            .unwrap();

        delete_scope(&agent, &agent.test_setup_config.bucket, &scope_name)
            .await
            .unwrap();
    });
}

async fn load_scoped_search_index(
    index_name: &str,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) -> Index {
    let mut data = include_str!("./testdata/basic_scoped_search_index.json");
    let mut data = data.replace("$indexName", index_name);
    let mut data = data.replace("$bucketName", bucket_name);
    let mut data = data.replace("$scopeName", scope_name);
    let data = data.replace("$collectionName", collection_name);

    let mut index: Index = serde_json::from_str(&data).unwrap();

    index
}

async fn load_search_index(index_name: &str, bucket_name: &str) -> Index {
    let mut data = include_str!("./testdata/basic_search_index.json");
    let mut data = data.replace("$indexName", index_name);
    let mut data = data.replace("$bucketName", bucket_name);

    let mut index: Index = serde_json::from_str(&data).unwrap();

    index
}
