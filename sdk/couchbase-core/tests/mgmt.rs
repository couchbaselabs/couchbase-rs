use crate::common::default_agent_options::create_default_options;
use crate::common::feature_supported;
use crate::common::helpers::generate_string_value;
use crate::common::test_config::{setup_tests, test_bucket};
use couchbase_core::agent::Agent;
use couchbase_core::cbconfig::CollectionManifest;
use couchbase_core::features::BucketFeature;
use couchbase_core::mgmtoptions::{
    CreateCollectionOptions, CreateScopeOptions, DeleteCollectionOptions, DeleteScopeOptions,
    EnsureManifestOptions, GetCollectionManifestOptions,
};
use couchbase_core::mgmtx::responses::{
    CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
};
use couchbase_core::{cbconfig, error};
use std::ops::Deref;

mod common;

#[tokio::test]
async fn test_scopes() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let name = generate_string_value(10);
    let bucket_name = test_bucket().await;

    let resp = create_scope(&agent, &bucket_name, &name).await.unwrap();
    assert!(!resp.manifest_uid.is_empty());

    ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

    let manifest = get_manifest(&agent, &bucket_name).await.unwrap();

    let scope_found = find_scope(&manifest, &name);
    assert!(scope_found.is_some());
    let scope_found = scope_found.unwrap();
    assert!(!scope_found.uid.is_empty());
    assert!(scope_found.collections.is_empty());

    let resp = delete_scope(&agent, &bucket_name, &name).await.unwrap();
    assert!(!resp.manifest_uid.is_empty());

    ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

    let manifest = get_manifest(&agent, &bucket_name).await.unwrap();

    let scope_found = find_scope(&manifest, &name);
    assert!(scope_found.is_none());

    agent.close().await;
}

#[tokio::test]
async fn test_collections() {
    setup_tests().await;

    let agent_opts = create_default_options().await;

    let mut agent = Agent::new(agent_opts).await.unwrap();

    let history_supported = feature_supported(&agent, BucketFeature::NonDedupedHistory).await;

    let scope_name = generate_string_value(10);
    let collection_name = generate_string_value(10);
    let bucket_name = test_bucket().await;

    let resp = create_scope(&agent, &bucket_name, &scope_name)
        .await
        .unwrap();
    assert!(!resp.manifest_uid.is_empty());

    ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

    let mut builder = CreateCollectionOptions::builder()
        .bucket_name(bucket_name.as_str())
        .scope_name(&*scope_name)
        .collection_name(&*collection_name)
        .max_ttl(25);

    let opts = if history_supported {
        builder.history_enabled(true).build()
    } else {
        builder.build()
    };

    let resp = agent.create_collection(&opts).await.unwrap();
    assert!(!resp.manifest_uid.is_empty());

    ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

    let manifest = get_manifest(&agent, &bucket_name).await.unwrap();
    assert!(!manifest.uid.is_empty());

    let scope_found = find_scope(&manifest, &scope_name).unwrap();
    let mut collection_found = find_collection(&scope_found, &collection_name);

    assert!(collection_found.is_some());
    let collection_found = collection_found.unwrap();
    assert_eq!(collection_found.max_ttl, Some(25));
    if history_supported {
        assert_eq!(collection_found.history, Some(true));
    } else {
        // Depending on server version the collection may have inherited the bucket default.
        assert!(collection_found.history.is_none() || collection_found.history == Some(false));
    }

    let resp = delete_collection(&agent, &bucket_name, &scope_name, &collection_name)
        .await
        .unwrap();
    assert!(!resp.manifest_uid.is_empty());

    ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

    let manifest = get_manifest(&agent, &bucket_name).await.unwrap();

    let mut scope_found = find_scope(&manifest, &scope_name);
    let scope_found = scope_found.unwrap();
    let mut collection_found = find_collection(&scope_found, &collection_name);
    assert!(collection_found.is_none());

    let resp = delete_scope(&agent, &bucket_name, &scope_name)
        .await
        .unwrap();
    assert!(!resp.manifest_uid.is_empty());

    agent.close().await;
}

fn find_scope(
    manifest: &cbconfig::CollectionManifest,
    scope_name: &str,
) -> Option<cbconfig::CollectionManifestScope> {
    for scope in &manifest.scopes {
        if scope.name == scope_name {
            return Some(scope.clone());
        }
    }
    None
}

fn find_collection(
    scope: &cbconfig::CollectionManifestScope,
    collection_name: &str,
) -> Option<cbconfig::CollectionManifestCollection> {
    for collection in &scope.collections {
        if collection.name == collection_name {
            return Some(collection.clone());
        }
    }
    None
}

async fn ensure_manifest(agent: &Agent, bucket_name: &str, manifest_uid: String) {
    let ensure_opts = &EnsureManifestOptions::builder()
        .bucket_name(bucket_name)
        .manifest_uid(u64::from_str_radix(&manifest_uid, 16).unwrap())
        .build();
    agent.ensure_manifest(ensure_opts).await.unwrap();
}

async fn delete_scope(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
) -> error::Result<DeleteScopeResponse> {
    let opts = &DeleteScopeOptions::builder()
        .bucket_name(bucket_name)
        .scope_name(scope_name)
        .build();
    agent.delete_scope(opts).await
}

async fn create_scope(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
) -> error::Result<CreateScopeResponse> {
    let opts = &CreateScopeOptions::builder()
        .bucket_name(bucket_name)
        .scope_name(scope_name)
        .build();
    agent.create_scope(opts).await
}

async fn get_manifest(agent: &Agent, bucket_name: &str) -> error::Result<CollectionManifest> {
    let opts = &GetCollectionManifestOptions::builder()
        .bucket_name(bucket_name)
        .build();
    agent.get_collection_manifest(opts).await
}

async fn delete_collection(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) -> error::Result<DeleteCollectionResponse> {
    let opts = &DeleteCollectionOptions::builder()
        .bucket_name(bucket_name)
        .scope_name(scope_name)
        .collection_name(collection_name)
        .build();
    agent.delete_collection(opts).await
}
