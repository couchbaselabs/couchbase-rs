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

use crate::common::consistency_utils::{
    verify_collection_created, verify_collection_dropped, verify_scope_created,
    verify_scope_dropped,
};
use crate::common::features::TestFeatureCode;
use crate::common::test_config::run_test;
use crate::common::{generate_string_value, try_until};
use couchbase::management::collections::collection_settings::{
    CreateCollectionSettings, MaxExpiryValue, UpdateCollectionSettings,
};
use std::time::Duration;

mod common;

#[test]
fn test_create_scope() {
    run_test(async |cluster, bucket| {
        let manager = bucket.collections();

        let name = generate_string_value(10);
        manager.create_scope(&name, None).await.unwrap();

        verify_scope_created(&manager, &name).await;
    })
}

#[test]
fn test_drop_scope() {
    run_test(async |cluster, bucket| {
        let manager = bucket.collections();

        let name = generate_string_value(10);
        manager.create_scope(&name, None).await.unwrap();

        verify_scope_created(&manager, &name).await;

        manager.drop_scope(&name, None).await.unwrap();

        verify_scope_dropped(&manager, &name).await;
    })
}

#[test]
fn test_create_collection() {
    run_test(async |cluster, bucket| {
        let manager = bucket.collections();

        let scope_name = generate_string_value(10);
        let collection_name = generate_string_value(10);

        manager.create_scope(&scope_name, None).await.unwrap();
        verify_scope_created(&manager, &scope_name).await;

        let mut settings = CreateCollectionSettings::new();
        if cluster.supports_feature(&TestFeatureCode::CollectionMaxExpiry) {
            settings = settings.max_expiry(MaxExpiryValue::Seconds(Duration::from_secs(2000)));
        }

        manager
            .create_collection(&scope_name, &collection_name, settings, None)
            .await
            .unwrap();

        let collection = verify_collection_created(&manager, &scope_name, &collection_name).await;

        assert_eq!(collection_name, collection.name());
        assert_eq!(scope_name, collection.scope_name());
        if cluster.supports_feature(&TestFeatureCode::CollectionMaxExpiry) {
            assert_eq!(
                MaxExpiryValue::Seconds(Duration::from_secs(2000)),
                collection.max_expiry()
            );
        }
        assert!(!collection.history());
    })
}

#[test]
fn test_update_collection() {
    run_test(async |cluster, bucket| {
        if !cluster.supports_feature(&TestFeatureCode::CollectionUpdateMaxExpiry) {
            return;
        }

        let manager = bucket.collections();

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
    run_test(async |cluster, bucket| {
        let manager = bucket.collections();

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
