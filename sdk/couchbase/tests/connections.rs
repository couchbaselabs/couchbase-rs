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
use crate::common::{new_key, try_until};
use couchbase::error::ErrorKind;
use std::ops::Add;
use std::time::Duration;
use tokio::time::Instant;

mod common;

// Tests in this file use try_until as it takes some time for the drop chain to be realized.

#[test]
fn test_collection_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let collection = {
            let cluster = create_test_cluster().await;

            cluster
                .bucket(cluster.default_bucket())
                .scope(cluster.default_scope())
                .collection(cluster.default_collection())
        };

        let key = new_key();

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
            Duration::from_millis(10),
            "operation didn't fail with disconnected!",
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

#[test]
fn test_collection_level_mgr_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let mgr = {
            let cluster = create_test_cluster().await;

            let collection = cluster
                .bucket(cluster.default_bucket())
                .scope(cluster.default_scope())
                .collection(cluster.default_collection());

            collection.query_indexes()
        };

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
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

#[test]
fn test_scope_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let scope = {
            let cluster = create_test_cluster().await;

            cluster
                .bucket(cluster.default_bucket())
                .scope(cluster.default_scope())
        };

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
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

#[test]
fn test_scope_level_mgr_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let mgr = {
            let cluster = create_test_cluster().await;

            let scope = cluster
                .bucket(cluster.default_bucket())
                .scope(cluster.default_scope());

            scope.search_indexes()
        };

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
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

#[test]
fn test_bucket_level_mgr_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let mgr = {
            let cluster = create_test_cluster().await;

            let bucket = cluster.bucket(cluster.default_bucket());

            bucket.collections()
        };

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
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

#[test]
fn test_cluster_level_mgr_use_after_cluster_drop() {
    run_test(async |_cluster| {
        let mgr = {
            let cluster = create_test_cluster().await;

            cluster.users()
        };

        try_until(
            Instant::now().add(Duration::from_millis(1000)),
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
