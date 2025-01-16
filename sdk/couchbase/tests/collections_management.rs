#![feature(async_closure)]

use crate::common::features::TestFeatureCode;
use crate::common::test_config::run_test;
use crate::common::{generate_string_value, try_until};
use couchbase::collections_manager::{
    CollectionManager, CreateCollectionSettings, MaxExpiryValue, UpdateCollectionSettings,
};
use couchbase::results::collections_mgmt_results::CollectionSpec;
use std::time::Duration;

mod common;

#[test]
fn test_create_scope() {
    run_test(async |cluster| {
        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let name = generate_string_value(10);
        manager.create_scope(&name, None).await.unwrap();

        verify_scope_created(&manager, &name).await;
    })
}

#[test]
fn test_drop_scope() {
    run_test(async |cluster| {
        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let name = generate_string_value(10);
        manager.create_scope(&name, None).await.unwrap();

        verify_scope_created(&manager, &name).await;

        manager.drop_scope(&name, None).await.unwrap();

        verify_scope_dropped(&manager, &name).await;
    })
}

#[test]
fn test_create_collection() {
    run_test(async |cluster| {
        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let scope_name = generate_string_value(10);
        let collection_name = generate_string_value(10);

        manager.create_scope(&scope_name, None).await.unwrap();
        verify_scope_created(&manager, &scope_name).await;

        let settings = CreateCollectionSettings::new()
            .max_expiry(MaxExpiryValue::Seconds(Duration::from_secs(2000)));

        manager
            .create_collection(&scope_name, &collection_name, settings, None)
            .await
            .unwrap();

        let collection = verify_collection_created(&manager, &scope_name, &collection_name).await;

        assert_eq!(collection_name, collection.name());
        assert_eq!(scope_name, collection.scope_name());
        assert_eq!(
            MaxExpiryValue::Seconds(Duration::from_secs(2000)),
            collection.max_expiry()
        );
        assert!(!collection.history());
    })
}

#[test]
fn test_update_collection() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::CollectionUpdates) {
            return;
        }

        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let scope_name = generate_string_value(10);
        let collection_name = generate_string_value(10);
        manager.create_scope(&scope_name, None).await.unwrap();
        verify_scope_created(&manager, &scope_name).await;

        let settings = CreateCollectionSettings::new()
            .max_expiry(MaxExpiryValue::Seconds(Duration::from_secs(2000)));

        manager
            .create_collection(&scope_name, &collection_name, settings, None)
            .await
            .unwrap();
        verify_collection_created(&manager, &scope_name, &collection_name).await;

        let settings = UpdateCollectionSettings::new()
            .max_expiry(MaxExpiryValue::Seconds(Duration::from_secs(7000)));

        manager
            .update_collection(&scope_name, &collection_name, settings, None)
            .await
            .unwrap();

        let collection = try_until(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_millis(100),
            "Collection was not updated in time",
            || async {
                let scopes = manager.get_all_scopes(None).await.unwrap();
                if !scopes.iter().any(|s| s.name() == scope_name) {
                    return Ok(None);
                };
                let scope = scopes.iter().find(|s| s.name() == scope_name).unwrap();
                let collection = scope
                    .collections()
                    .iter()
                    .find(|c| c.name() == collection_name)
                    .unwrap();

                if collection.max_expiry() != MaxExpiryValue::Seconds(Duration::from_secs(7000)) {
                    return Ok(None);
                }

                Ok(Some(collection.clone()))
            },
        )
        .await;

        assert_eq!(collection_name, collection.name());
        assert_eq!(scope_name, collection.scope_name());
        assert_eq!(
            MaxExpiryValue::Seconds(Duration::from_secs(7000)),
            collection.max_expiry()
        );
        assert!(!collection.history());
    })
}

#[test]
fn test_drop_collection() {
    run_test(async |cluster| {
        let manager = cluster.bucket(&cluster.default_bucket).collections();

        let scope_name = generate_string_value(10);
        let collection_name = generate_string_value(10);

        manager.create_scope(&scope_name, None).await.unwrap();
        verify_scope_created(&manager, &scope_name).await;

        let create_settings = CreateCollectionSettings::new();

        manager
            .create_collection(&scope_name, &collection_name, create_settings, None)
            .await
            .unwrap();
        verify_collection_created(&manager, &scope_name, &collection_name).await;

        manager
            .drop_collection(&scope_name, &collection_name, None)
            .await
            .unwrap();
        verify_collection_dropped(&manager, &scope_name, &collection_name).await;
    })
}

async fn verify_scope_created(manager: &CollectionManager, scope_name: &str) {
    try_until(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_millis(100),
        "Scope was not created in time",
        || async {
            let scopes = manager.get_all_scopes(None).await.unwrap();
            if !scopes.iter().any(|s| s.name() == scope_name) {
                return Ok(None);
            }

            Ok(Some(()))
        },
    )
    .await;
}

async fn verify_scope_dropped(manager: &CollectionManager, scope_name: &str) {
    try_until(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_millis(100),
        "Scope was not dropped in time",
        || async {
            let scopes = manager.get_all_scopes(None).await.unwrap();
            if scopes.iter().any(|s| s.name() == scope_name) {
                return Ok(None);
            };

            Ok(Some(()))
        },
    )
    .await;
}

async fn verify_collection_created(
    manager: &CollectionManager,
    scope_name: &str,
    collection_name: &str,
) -> CollectionSpec {
    try_until(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_millis(100),
        "Collection was not created in time",
        || async {
            let scopes = manager.get_all_scopes(None).await.unwrap();
            if !scopes.iter().any(|s| s.name() == scope_name) {
                return Ok(None);
            };
            let scope = scopes.iter().find(|s| s.name() == scope_name).unwrap();
            let collection = scope
                .collections()
                .iter()
                .find(|c| c.name() == collection_name);

            Ok(collection.cloned())
        },
    )
    .await
}

async fn verify_collection_dropped(
    manager: &CollectionManager,
    scope_name: &str,
    collection_name: &str,
) {
    try_until(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_millis(100),
        "Collection was not dropped in time",
        || async {
            let scopes = manager.get_all_scopes(None).await.unwrap();
            let scope = scopes.iter().find(|s| s.name() == scope_name).unwrap();
            if scope
                .collections()
                .iter()
                .any(|c| c.name() == collection_name)
            {
                return Ok(None);
            }

            Ok(Some(()))
        },
    )
    .await;
}
