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
use crate::tracing::SERVICE_VALUE_QUERY;
use couchbase_core::create_span;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct QueryIndexManager {
    pub(crate) client: Arc<QueryIndexMgmtClient>,
}

impl QueryIndexManager {
    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllQueryIndexesOptions>>,
    ) -> error::Result<Vec<QueryIndex>> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_get_all_indexes"),
            )
            .await;
        let result = self
            .client
            .get_all_indexes(opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: impl Into<Vec<String>>,
        opts: impl Into<Option<CreateQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_create_index"),
            )
            .await;
        let result = self
            .client
            .create_index(index_name.into(), fields.into(), opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn create_primary_index(
        &self,
        opts: impl Into<Option<CreatePrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_create_primary_index"),
            )
            .await;
        let result = self
            .client
            .create_primary_index(opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_drop_index"),
            )
            .await;
        let result = self
            .client
            .drop_index(index_name.into(), opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn drop_primary_index(
        &self,
        opts: impl Into<Option<DropPrimaryQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_drop_primary_index"),
            )
            .await;
        let result = self
            .client
            .drop_primary_index(opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn watch_indexes(
        &self,
        index_names: impl Into<Vec<String>>,
        opts: impl Into<Option<WatchQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_watch_indexes"),
            )
            .await;
        let result = self
            .client
            .watch_indexes(index_names.into(), opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: impl Into<Option<BuildQueryIndexOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_QUERY),
                self.client.keyspace(),
                create_span!("manager_query_build_deferred_indexes"),
            )
            .await;
        let result = self
            .client
            .build_deferred_indexes(opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
