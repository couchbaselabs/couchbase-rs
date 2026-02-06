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

use crate::clients::query_index_mgmt_client::QueryIndexMgmtClient;
use crate::error;
use crate::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllQueryIndexesOptions,
    WatchQueryIndexOptions,
};
use crate::results::query_index_mgmt_results::QueryIndex;
use crate::tracing::SpanBuilder;
use crate::tracing::{
    SERVICE_VALUE_QUERY, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct QueryIndexManager {
    pub(crate) client: Arc<QueryIndexMgmtClient>,
}

impl QueryIndexManager {
    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllQueryIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        self.get_all_indexes_internal(opts).await
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.create_index_internal(index_name, fields, opts).await
    }

    pub async fn create_primary_index(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.create_primary_index_internal(opts).await
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.drop_index_internal(index_name, opts).await
    }

    pub async fn drop_primary_index(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.drop_primary_index_internal(opts).await
    }

    pub async fn watch_indexes(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.watch_indexes_internal(index_names, opts).await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.build_deferred_indexes_internal(opts).await
    }

    async fn get_all_indexes_internal(
        &self,
        opts: impl Into<Option<GetAllQueryIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_get_all_indexes"),
                self.client.get_all_indexes(opts.into()),
            )
            .await
    }

    async fn create_index_internal(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_create_index"),
                self.client
                    .create_index(index_name.into(), fields.into(), opts.into()),
            )
            .await
    }

    async fn create_primary_index_internal(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_create_primary_index"),
                self.client.create_primary_index(opts.into()),
            )
            .await
    }

    async fn drop_index_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_drop_index"),
                self.client.drop_index(index_name.into(), opts.into()),
            )
            .await
    }

    async fn drop_primary_index_internal(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_drop_primary_index"),
                self.client.drop_primary_index(opts.into()),
            )
            .await
    }

    async fn watch_indexes_internal(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_watch_indexes"),
                self.client.watch_indexes(index_names.into(), opts.into()),
            )
            .await
    }

    async fn build_deferred_indexes_internal(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &self.client.keyspace(),
                create_span!("manager_query_build_deferred_indexes"),
                self.client.build_deferred_indexes(opts.into()),
            )
            .await
    }
}
