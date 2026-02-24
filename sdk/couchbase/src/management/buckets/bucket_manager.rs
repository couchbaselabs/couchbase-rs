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
use crate::tracing::Keyspace;
use crate::tracing::SpanBuilder;
use crate::tracing::{
    SERVICE_VALUE_MANAGEMENT, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use couchbase_core::create_span;
use std::sync::Arc;

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
        self.get_all_buckets_internal(opts).await
    }

    pub async fn get_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<GetBucketOptions>>,
    ) -> error::Result<BucketSettings> {
        self.get_bucket_internal(bucket_name.into(), opts).await
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<CreateBucketOptions>>,
    ) -> error::Result<()> {
        self.create_bucket_internal(settings, opts).await
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<UpdateBucketOptions>>,
    ) -> error::Result<()> {
        self.update_bucket_internal(settings, opts).await
    }

    pub async fn drop_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<DropBucketOptions>>,
    ) -> error::Result<()> {
        self.drop_bucket_internal(bucket_name.into(), opts).await
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: impl Into<String>,
        opts: impl Into<Option<FlushBucketOptions>>,
    ) -> error::Result<()> {
        self.flush_bucket_internal(bucket_name.into(), opts).await
    }

    async fn get_all_buckets_internal(
        &self,
        opts: impl Into<Option<GetAllBucketsOptions>>,
    ) -> error::Result<Vec<BucketSettings>> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &Keyspace::Cluster,
                create_span!("manager_buckets_get_all_buckets"),
                || self.client.get_all_buckets(opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn get_bucket_internal(
        &self,
        bucket_name: String,
        opts: impl Into<Option<GetBucketOptions>>,
    ) -> error::Result<BucketSettings> {
        let keyspace = Keyspace::Bucket {
            bucket: bucket_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_buckets_get_bucket"),
                || self.client
                    .get_bucket(bucket_name, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn create_bucket_internal(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<CreateBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: settings.name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_buckets_create_bucket"),
                || self.client
                    .create_bucket(settings, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn update_bucket_internal(
        &self,
        settings: BucketSettings,
        opts: impl Into<Option<UpdateBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: settings.name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_buckets_update_bucket"),
                || self.client
                    .update_bucket(settings, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn drop_bucket_internal(
        &self,
        bucket_name: String,
        opts: impl Into<Option<DropBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: bucket_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_buckets_drop_bucket"),
                || self.client
                    .drop_bucket(bucket_name, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn flush_bucket_internal(
        &self,
        bucket_name: String,
        opts: impl Into<Option<FlushBucketOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Bucket {
            bucket: bucket_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_buckets_flush_bucket"),
                || self.client
                    .flush_bucket(bucket_name, opts.into().unwrap_or_default()),
            )
            .await
    }
}
