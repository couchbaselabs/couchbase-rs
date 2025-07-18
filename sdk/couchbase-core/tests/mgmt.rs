use crate::common::helpers::{
    create_scope, delete_collection, delete_scope, feature_supported, generate_string_value,
    try_until,
};
use crate::common::test_config::run_test;
use couchbase_core::agent::Agent;
use couchbase_core::cbconfig::CollectionManifest;
use couchbase_core::features::BucketFeature;
use couchbase_core::mgmtx::bucket_settings::{BucketSettings, BucketType};
use couchbase_core::options::management::{
    CreateBucketOptions, CreateCollectionOptions, DeleteBucketOptions, EnsureBucketOptions,
    EnsureManifestOptions, GetBucketOptions, GetCollectionManifestOptions, UpdateBucketOptions,
};
use couchbase_core::{cbconfig, error};
use serial_test::serial;
use std::future::Future;
use std::ops::{Add, Deref};
use std::time::Duration;
use tokio::time::Instant;

mod common;

#[test]
fn test_scopes() {
    run_test(async |mut agent| {
        let name = generate_string_value(10);
        let bucket_name = agent.test_setup_config.bucket.clone();

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
    });
}

#[test]
fn test_collections() {
    run_test(async |mut agent| {
        let history_supported = feature_supported(&agent, BucketFeature::NonDedupedHistory).await;

        let scope_name = generate_string_value(10);
        let collection_name = generate_string_value(10);
        let bucket_name = agent.test_setup_config.bucket.clone();

        let resp = create_scope(&agent, &bucket_name, &scope_name)
            .await
            .unwrap();
        assert!(!resp.manifest_uid.is_empty());

        ensure_manifest(&agent, &bucket_name, resp.manifest_uid).await;

        let mut opts =
            CreateCollectionOptions::new(&bucket_name, &scope_name, &collection_name).max_ttl(25);

        if history_supported {
            opts = opts.history_enabled(true)
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
    });
}

#[serial]
#[test]
fn test_buckets() {
    run_test(async |mut agent| {
        let bucket_name = generate_string_value(10);

        let settings = BucketSettings::default()
            .bucket_type(BucketType::EPHEMERAL)
            .ram_quota_mb(100);

        let opts = &CreateBucketOptions::new(&bucket_name, &settings);

        agent.create_bucket(opts).await.unwrap();
    });
}

#[serial]
#[test]
fn test_create_couchbase_bucket() {
    run_test(async |mut agent| {
        let bucket_name = generate_string_value(10);

        let settings = BucketSettings::default()
            .bucket_type(BucketType::COUCHBASE)
            .ram_quota_mb(100);

        let opts = &CreateBucketOptions::new(&bucket_name, &settings);

        agent.create_bucket(opts).await.unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let get_opts = &GetBucketOptions::new(&bucket_name);
        let bucket = agent.get_bucket(get_opts).await.unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(
            bucket.bucket_settings.bucket_type,
            Some(BucketType::COUCHBASE)
        );

        agent.close().await;
    });
}

#[serial]
#[test]
fn test_create_ephemeral_bucket() {
    run_test(async |mut agent| {
        let bucket_name = generate_string_value(10);

        let settings = BucketSettings::default()
            .bucket_type(BucketType::EPHEMERAL)
            .ram_quota_mb(100);

        let opts = &CreateBucketOptions::new(&bucket_name, &settings);

        agent.create_bucket(opts).await.unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let get_opts = &GetBucketOptions::new(&bucket_name);
        let bucket = agent.get_bucket(get_opts).await.unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(
            bucket.bucket_settings.bucket_type,
            Some(BucketType::EPHEMERAL)
        );

        agent.close().await;
    });
}

#[serial]
#[test]
fn test_update_bucket() {
    run_test(async |mut agent| {
        let bucket_name = generate_string_value(10);

        let settings = BucketSettings::default()
            .bucket_type(BucketType::COUCHBASE)
            .ram_quota_mb(100);

        let create_opts = &CreateBucketOptions::new(&bucket_name, &settings);
        agent.create_bucket(create_opts).await.unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let update_settings = BucketSettings::default()
            .ram_quota_mb(200)
            .max_ttl(Duration::from_secs(3600));

        agent
            .update_bucket(&UpdateBucketOptions::new(&bucket_name, &update_settings))
            .await
            .unwrap();

        let get_opts = &GetBucketOptions::new(&bucket_name);
        let bucket = try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(100),
            "bucket update not applied within time",
            || async {
                let bucket = match agent.get_bucket(get_opts).await {
                    Ok(b) => b,
                    Err(_e) => return Ok(None),
                };

                if bucket.bucket_settings.ram_quota_mb == Some(200) {
                    return Ok(Some(bucket));
                }

                Ok(None)
            },
        )
        .await;

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(200));
        assert_eq!(
            bucket.bucket_settings.max_ttl,
            Some(Duration::from_secs(3600))
        );

        agent.close().await;
    });
}

#[serial]
#[test]
fn test_delete_bucket() {
    run_test(async |mut agent| {
        let bucket_name = generate_string_value(10);

        let settings = BucketSettings::default()
            .bucket_type(BucketType::EPHEMERAL)
            .ram_quota_mb(100);

        let create_opts = &CreateBucketOptions::new(&bucket_name, &settings);
        agent.create_bucket(create_opts).await.unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let delete_opts = &DeleteBucketOptions::new(&bucket_name);
        agent.delete_bucket(delete_opts).await.unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        let get_opts = &GetBucketOptions::new(&bucket_name);
        let result = agent.get_bucket(get_opts).await;

        assert!(result.is_err());
    });
}

fn find_scope(
    manifest: &CollectionManifest,
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
    let ensure_opts =
        &EnsureManifestOptions::new(bucket_name, u64::from_str_radix(&manifest_uid, 16).unwrap());

    agent.ensure_manifest(ensure_opts).await.unwrap();
}

async fn get_manifest(agent: &Agent, bucket_name: &str) -> error::Result<CollectionManifest> {
    let opts = &GetCollectionManifestOptions::new(bucket_name);
    agent.get_collection_manifest(opts).await
}
