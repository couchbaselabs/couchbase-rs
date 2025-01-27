use crate::common::try_until;
use couchbase::collections_manager::CollectionManager;
use couchbase::results::collections_mgmt_results::CollectionSpec;
use std::time::Duration;

pub async fn verify_scope_created(manager: &CollectionManager, scope_name: &str) {
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

pub async fn verify_scope_dropped(manager: &CollectionManager, scope_name: &str) {
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

pub async fn verify_collection_created(
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

pub async fn verify_collection_dropped(
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
