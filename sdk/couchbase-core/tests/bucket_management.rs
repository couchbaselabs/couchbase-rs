/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::common::features::TestFeatureCode;
use crate::common::helpers::{generate_string_value, try_until};
use crate::common::test_config::run_test;
use couchbase_core::mgmtx::bucket_settings::{
    BucketSettings, BucketType, CompressionMode, ConflictResolutionType, DurabilityLevel,
    EvictionPolicyType, StorageBackend,
};
use couchbase_core::options::management::{
    CreateBucketOptions, DeleteBucketOptions, EnsureBucketOptions, GetBucketOptions,
    UpdateBucketOptions,
};
use serial_test::serial;
use std::ops::Add;
use std::time::Duration;
use tokio::time::Instant;

mod common;

#[serial]
#[test]
fn test_create_bucket() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
    })
}

#[serial]
#[test]
fn test_update_bucket() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let updated_settings = settings.ram_quota_mb(200);
        agent
            .update_bucket(&UpdateBucketOptions::new(&bucket_name, &updated_settings))
            .await
            .unwrap();

        try_until(
            Instant::now().add(Duration::from_secs(30)),
            Duration::from_millis(100),
            "Bucket was not updated in time",
            || async {
                let bucket = agent
                    .get_bucket(&GetBucketOptions::new(&bucket_name))
                    .await?;

                if bucket.bucket_settings.ram_quota_mb == Some(200) {
                    Ok(Some(bucket))
                } else {
                    Ok(None)
                }
            },
        )
        .await;

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();
    })
}

#[serial]
#[test]
fn test_delete_bucket() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();
    })
}

#[serial]
#[test]
fn test_create_bucket_with_replica_number() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true)
            .replica_number(2);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
        assert_eq!(bucket.bucket_settings.replica_number, Some(2));
    })
}

#[serial]
#[test]
fn test_create_bucket_with_eviction_policy() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true)
            .eviction_policy(EvictionPolicyType::VALUE_ONLY);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
        assert_eq!(
            bucket.bucket_settings.eviction_policy,
            Some(EvictionPolicyType::VALUE_ONLY)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_compression_mode() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true)
            .compression_mode(CompressionMode::ACTIVE);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
        assert_eq!(
            bucket.bucket_settings.compression_mode,
            Some(CompressionMode::ACTIVE)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_durability_min_level() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true)
            .durability_min_level(DurabilityLevel::MAJORITY);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
        assert_eq!(
            bucket.bucket_settings.durability_min_level,
            Some(DurabilityLevel::MAJORITY)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_conflict_resolution_type() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement) {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .ram_quota_mb(100)
            .flush_enabled(true)
            .conflict_resolution_type(ConflictResolutionType::TIMESTAMP);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(100));
        assert_eq!(bucket.bucket_settings.flush_enabled, Some(true));
        assert_eq!(
            bucket.bucket_settings.conflict_resolution_type,
            Some(ConflictResolutionType::TIMESTAMP)
        );
    })
}

#[serial]
#[test]
fn test_create_bucket_with_history_retention() {
    run_test(async |mut agent| {
        if !agent.supports_feature(&TestFeatureCode::BucketManagement)
            || !agent.supports_feature(&TestFeatureCode::HistoryRetention)
        {
            return;
        }

        let bucket_name = generate_string_value(10);
        let settings = BucketSettings::default()
            .bucket_type(BucketType::COUCHBASE)
            .storage_backend(StorageBackend::MAGMA)
            .ram_quota_mb(1024)
            .history_retention_collection_default(true)
            .history_retention_bytes(2147483648)
            .history_retention_seconds(3600);

        agent
            .create_bucket(&CreateBucketOptions::new(&bucket_name, &settings))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, false))
            .await
            .unwrap();

        let bucket = agent
            .get_bucket(&GetBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .delete_bucket(&DeleteBucketOptions::new(&bucket_name))
            .await
            .unwrap();

        agent
            .ensure_bucket(&EnsureBucketOptions::new(&bucket_name, true))
            .await
            .unwrap();

        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.bucket_settings.ram_quota_mb, Some(1024));
        assert_eq!(
            bucket.bucket_settings.history_retention_collection_default,
            Some(true)
        );
        assert_eq!(
            bucket.bucket_settings.history_retention_bytes,
            Some(2147483648)
        );
        assert_eq!(bucket.bucket_settings.history_retention_seconds, Some(3600));
    })
}
