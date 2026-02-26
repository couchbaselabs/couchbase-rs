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

use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::error;
use crate::management::buckets::bucket_settings::BucketSettings;
use crate::options::bucket_mgmt_options::*;
use crate::tracing::{Keyspace, SERVICE_VALUE_MANAGEMENT};
use couchbase_core::create_span;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct BucketManager {
    client: Arc<BucketMgmtClient>,
}

impl BucketManager {
    pub(crate) fn new(client: Arc<BucketMgmtClient>) -> Self {
        Self { client }
    }

    pub async fn get_all_buckets(
        &self,
        opts: impl Into<Option<GetAllBucketsOptions>>,
    ) -> error::Result<Vec<BucketSettings>> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                Keyspace::Cluster,
                create_span!("manager_buckets_get_all_buckets"),
            )
            .await;
        let result = self
            .client
            .get_all_buckets(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<GetBucketOptions>>,
    ) -> error::Result<BucketSettings> {
        let bucket_name: String = bucket_name.into();
        let keyspace = Keyspace::Bucket {
            bucket: &bucket_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_buckets_get_bucket"),
            )
            .await;
        let result = self
            .client
            .get_bucket(bucket_name.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<CreateBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: &settings.name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_buckets_create_bucket"),
            )
            .await;
        let result = self
            .client
            .create_bucket(settings.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<UpdateBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: &settings.name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_buckets_update_bucket"),
            )
            .await;
        let result = self
            .client
            .update_bucket(settings.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn drop_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<DropBucketOptions>>,
    ) -> error::Result<()> {
        let bucket_name: String = bucket_name.into();
        let keyspace = Keyspace::Bucket {
            bucket: &bucket_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_buckets_drop_bucket"),
            )
            .await;
        let result = self
            .client
            .drop_bucket(bucket_name.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<FlushBucketOptions>>,
    ) -> error::Result<()> {
        let bucket_name: String = bucket_name.into();
        let keyspace = Keyspace::Bucket {
            bucket: &bucket_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_buckets_flush_bucket"),
            )
            .await;
        let result = self
            .client
            .flush_bucket(bucket_name.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
