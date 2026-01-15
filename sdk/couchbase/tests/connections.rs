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

use crate::common::test_config::{create_test_cluster, run_test};
use crate::common::{new_key, try_times};
use couchbase::error::ErrorKind;
use serial_test::serial;
use std::ops::Add;
use std::time::Duration;

mod common;

// Tests in this file use try_times as it takes some time for the drop chain to be realized,
// but by the time that the operation runs a second time it should be completed.

#[serial]
#[test]
fn test_collection_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let collection = {
            let cluster = create_test_cluster().await;

            let bucket = cluster.bucket(cluster.default_bucket());
            bucket.wait_until_ready(None).await.unwrap();

            bucket
                .scope(cluster.default_scope())
                .collection(cluster.default_collection())
        };

        let key = new_key();

        try_times(
            2,
            Duration::from_millis(1000),
            "operation didn't fail with disconnected",
            async || {
                let err = match collection.upsert(&key, "test", None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}

#[serial]
#[test]
fn test_collection_level_mgr_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let mgr = {
            let cluster = create_test_cluster().await;

            let bucket = cluster.bucket(cluster.default_bucket());

            let collection = bucket
                .scope(cluster.default_scope())
                .collection(cluster.default_collection());

            collection.query_indexes()
        };

        try_times(
            2,
            Duration::from_millis(10),
            "operation didn't fail with disconnected",
            async || {
                let err = match mgr.get_all_indexes(None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}

#[serial]
#[test]
fn test_scope_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let scope = {
            let cluster = create_test_cluster().await;
            let bucket = cluster.bucket(cluster.default_bucket());

            bucket.scope(cluster.default_scope())
        };

        try_times(
            2,
            Duration::from_millis(10),
            "operation didn't fail with disconnected",
            async || {
                let err = match scope.query("SELECT 1=1", None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}

#[serial]
#[test]
fn test_scope_level_mgr_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let mgr = {
            let cluster = create_test_cluster().await;

            let bucket = cluster.bucket(cluster.default_bucket());
            let scope = bucket.scope(cluster.default_scope());

            scope.search_indexes()
        };

        try_times(
            2,
            Duration::from_millis(10),
            "operation didn't fail with disconnected",
            async || {
                let err = match mgr.get_all_indexes(None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}

#[serial]
#[test]
fn test_bucket_level_mgr_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let mgr = {
            let cluster = create_test_cluster().await;
            let bucket = cluster.bucket(cluster.default_bucket());

            bucket.collections()
        };

        try_times(
            2,
            Duration::from_millis(10),
            "operation didn't fail with disconnected",
            async || {
                let err = match mgr.get_all_scopes(None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}

#[serial]
#[test]
fn test_cluster_level_mgr_use_after_cluster_drop() {
    run_test(async |cluster, _bucket| {
        let mgr = {
            let cluster = create_test_cluster().await;

            cluster.users()
        };

        try_times(
            2,
            Duration::from_millis(10),
            "operation didn't fail with disconnected",
            async || {
                let err = match mgr.get_all_groups(None).await {
                    Ok(_) => return Ok(None),
                    Err(e) => e,
                };

                if &ErrorKind::Disconnected == err.kind() {
                    return Ok(Some(()));
                }

                Ok(None)
            },
        )
        .await;
    })
}
