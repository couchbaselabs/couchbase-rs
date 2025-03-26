use crate::common::consistency_utils::{verify_bucket_created, verify_bucket_deleted};
use crate::common::features::TestFeatureCode;
use crate::common::test_config::run_test;
use crate::common::{generate_string_value, try_until};
use couchbase::durability_level::DurabilityLevel;
use couchbase::management::buckets::bucket_settings::{
    BucketSettings, CompressionMode, ConflictResolutionType, EvictionPolicyType, StorageBackend,
};
use serial_test::serial;
use std::time::Duration;

mod common;

#[serial]
#[test]
fn test_create_bucket() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100).flush_enabled(true);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
    })
}

#[serial]
#[test]
fn test_update_bucket() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100).flush_enabled(true);

        manager.create_bucket(settings.clone(), None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let updated_settings = settings.ram_quota_mb(200);
        manager.update_bucket(updated_settings, None).await.unwrap();

        try_until(
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_millis(100),
            "Bucket was not updated in time",
            || async {
                let bucket = manager.get_bucket(&bucket_name, None).await?;

                if bucket.ram_quota_mb == 200 {
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            },
        )
        .await;

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;
    })
}

#[serial]
#[test]
fn test_delete_bucket() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100).flush_enabled(true);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;
    })
}

#[serial]
#[test]
fn test_create_bucket_with_replica_number() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100)
            .flush_enabled(true)
            .num_replicas(2);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
        assert_eq!(bucket.num_replicas, Some(2));
    })
}

#[serial]
#[test]
fn test_create_bucket_with_eviction_policy() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100)
            .flush_enabled(true)
            .eviction_policy(EvictionPolicyType::VALUE_ONLY);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
        assert_eq!(bucket.eviction_policy, Some(EvictionPolicyType::VALUE_ONLY));
    })
}

#[serial]
#[test]
fn test_create_bucket_with_compression_mode() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100)
            .flush_enabled(true)
            .compression_mode(CompressionMode::ACTIVE);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
        assert_eq!(bucket.compression_mode, Some(CompressionMode::ACTIVE));
    })
}

#[serial]
#[test]
fn test_create_bucket_with_durability_min_level() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100)
            .flush_enabled(true)
            .minimum_durability_level(DurabilityLevel::MAJORITY);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
        assert_eq!(
            bucket.minimum_durability_level,
            Some(DurabilityLevel::MAJORITY)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_conflict_resolution_type() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 100)
            .flush_enabled(true)
            .conflict_resolution_type(ConflictResolutionType::TIMESTAMP);

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 100);
        assert_eq!(bucket.flush_enabled, Some(true));
        assert_eq!(
            bucket.conflict_resolution_type,
            Some(ConflictResolutionType::TIMESTAMP)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_history_retention() {
    run_test(async |cluster| {
        if !cluster.supports_feature(&TestFeatureCode::BucketManagement)
            || !cluster.supports_feature(&TestFeatureCode::HistoryRetention)
        {
            return;
        }

        let manager = cluster.buckets();

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::new(&bucket_name, 1024)
            .storage_backend(StorageBackend::MAGMA)
            .history_retention_collection_default(true)
            .history_retention_bytes(2147483648)
            .history_retention_duration(Duration::from_secs(3600));

        manager.create_bucket(settings, None).await.unwrap();

        verify_bucket_created(&manager, &bucket_name).await;

        let bucket = manager.get_bucket(&bucket_name, None).await.unwrap();

        manager.delete_bucket(&bucket_name, None).await.unwrap();
        verify_bucket_deleted(&manager, &bucket_name).await;

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.ram_quota_mb, 1024);
        assert_eq!(bucket.history_retention_collection_default, Some(true));
        assert_eq!(bucket.history_retention_bytes, Some(2147483648));
        assert_eq!(
            bucket.history_retention_duration,
            Some(Duration::from_secs(3600))
        );
    })
}
