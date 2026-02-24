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

use crate::clients::search_index_mgmt_client::SearchIndexMgmtClient;
use crate::error;
use crate::management::search::index::SearchIndex;
use crate::options::search_index_mgmt_options::{
    AllowQueryingSearchIndexOptions, AnalyzeDocumentOptions, DisallowQueryingSearchIndexOptions,
    DropSearchIndexOptions, FreezePlanSearchIndexOptions, GetAllSearchIndexesOptions,
    GetIndexedDocumentsCountOptions, GetSearchIndexOptions, PauseIngestSearchIndexOptions,
    ResumeIngestSearchIndexOptions, UnfreezePlanSearchIndexOptions, UpsertSearchIndexOptions,
};
use crate::tracing::{
    SpanBuilder, SERVICE_VALUE_SEARCH, SPAN_ATTRIB_DB_SYSTEM_VALUE,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use couchbase_core::create_span;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct SearchIndexManager {
    pub(crate) client: Arc<SearchIndexMgmtClient>,
}

impl SearchIndexManager {
    pub async fn get_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetSearchIndexOptions>>,
    ) -> error::Result<SearchIndex> {
        self.get_index_internal(index_name, opts).await
    }

    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllSearchIndexesOptions>>,
    ) -> error::Result<Vec<SearchIndex>> {
        self.get_all_indexes_internal(opts).await
    }

    pub async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: impl Into<Option<UpsertSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.upsert_index_internal(index, opts).await
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.drop_index_internal(index_name, opts).await
    }

    pub async fn analyze_document(
        &self,
        index_name: impl Into<String>,
        document: Value,
        opts: impl Into<Option<AnalyzeDocumentOptions>>,
    ) -> error::Result<Value> {
        self.analyze_document_internal(index_name, document, opts)
            .await
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetIndexedDocumentsCountOptions>>,
    ) -> error::Result<u64> {
        self.get_indexed_documents_count_internal(index_name, opts)
            .await
    }

    pub async fn pause_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<PauseIngestSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.pause_ingest_internal(index_name, opts).await
    }

    pub async fn resume_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<ResumeIngestSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.resume_ingest_internal(index_name, opts).await
    }

    pub async fn allow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<AllowQueryingSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.allow_querying_internal(index_name, opts).await
    }

    pub async fn disallow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DisallowQueryingSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.disallow_querying_internal(index_name, opts).await
    }

    pub async fn freeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<FreezePlanSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.freeze_plan_internal(index_name, opts).await
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<UnfreezePlanSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.unfreeze_plan_internal(index_name, opts).await
    }

    async fn get_index_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetSearchIndexOptions>>,
    ) -> error::Result<SearchIndex> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_get_index"),
                || self.client.get_index(index_name.into(), opts.into()),
            )
            .await
    }

    async fn get_all_indexes_internal(
        &self,
        opts: impl Into<Option<GetAllSearchIndexesOptions>>,
    ) -> error::Result<Vec<SearchIndex>> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_get_all_indexes"),
                || self.client.get_all_indexes(opts.into()),
            )
            .await
    }

    async fn upsert_index_internal(
        &self,
        index: SearchIndex,
        opts: impl Into<Option<UpsertSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_upsert_index"),
                || self.client.upsert_index(index, opts.into()),
            )
            .await
    }

    async fn drop_index_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DropSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_drop_index"),
                || self.client.drop_index(index_name.into(), opts.into()),
            )
            .await
    }

    async fn analyze_document_internal(
        &self,
        index_name: impl Into<String>,
        document: Value,
        opts: impl Into<Option<AnalyzeDocumentOptions>>,
    ) -> error::Result<Value> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_analyze_document"),
                || self.client
                    .analyze_document(index_name.into(), document, opts.into()),
            )
            .await
    }

    async fn get_indexed_documents_count_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetIndexedDocumentsCountOptions>>,
    ) -> error::Result<u64> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_get_indexed_documents_count"),
                || self.client
                    .get_indexed_documents_count(index_name.into(), opts.into()),
            )
            .await
    }

    async fn pause_ingest_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<PauseIngestSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_pause_ingest"),
                || self.client.pause_ingest(index_name.into(), opts.into()),
            )
            .await
    }

    async fn resume_ingest_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<ResumeIngestSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_resume_ingest"),
                || self.client.resume_ingest(index_name.into(), opts.into()),
            )
            .await
    }

    async fn allow_querying_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<AllowQueryingSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_allow_querying"),
                || self.client.allow_querying(index_name.into(), opts.into()),
            )
            .await
    }

    async fn disallow_querying_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DisallowQueryingSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_disallow_querying"),
                || self.client
                    .disallow_querying(index_name.into(), opts.into()),
            )
            .await
    }

    async fn freeze_plan_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<FreezePlanSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_freeze_plan"),
                || self.client.freeze_plan(index_name.into(), opts.into()),
            )
            .await
    }

    async fn unfreeze_plan_internal(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<UnfreezePlanSearchIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.client.keyspace(),
                create_span!("manager_search_unfreeze_plan"),
                || self.client.unfreeze_plan(index_name.into(), opts.into()),
            )
            .await
    }
}
