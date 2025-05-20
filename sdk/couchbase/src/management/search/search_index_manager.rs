use crate::clients::search_index_mgmt_client::SearchIndexMgmtClient;
use crate::error;
use crate::management::search::index::SearchIndex;
use crate::options::search_index_mgmt_options::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetAllIndexesOptions, GetIndexOptions, GetIndexedDocumentsCountOptions,
    PauseIngestOptions, ResumeIngestOptions, UnfreezePlanOptions, UpsertIndexOptions,
};
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
        opts: impl Into<Option<GetIndexOptions>>,
    ) -> error::Result<SearchIndex> {
        self.client.get_index(index_name.into(), opts.into()).await
    }

    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllIndexesOptions>>,
    ) -> error::Result<Vec<SearchIndex>> {
        self.client.get_all_indexes(opts.into()).await
    }

    pub async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: impl Into<Option<UpsertIndexOptions>>,
    ) -> error::Result<()> {
        self.client.upsert_index(index, opts.into()).await
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DeleteIndexOptions>>,
    ) -> error::Result<()> {
        self.client
            .delete_index(index_name.into(), opts.into())
            .await
    }

    pub async fn analyze_document(
        &self,
        index_name: impl Into<String>,
        document: Value,
        opts: impl Into<Option<AnalyzeDocumentOptions>>,
    ) -> error::Result<Value> {
        self.client
            .analyze_document(index_name.into(), document, opts.into())
            .await
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetIndexedDocumentsCountOptions>>,
    ) -> error::Result<u64> {
        self.client
            .get_indexed_documents_count(index_name.into(), opts.into())
            .await
    }

    pub async fn pause_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<PauseIngestOptions>>,
    ) -> error::Result<()> {
        self.client
            .pause_ingest(index_name.into(), opts.into())
            .await
    }

    pub async fn resume_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<ResumeIngestOptions>>,
    ) -> error::Result<()> {
        self.client
            .resume_ingest(index_name.into(), opts.into())
            .await
    }

    pub async fn allow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<AllowQueryingOptions>>,
    ) -> error::Result<()> {
        self.client
            .allow_querying(index_name.into(), opts.into())
            .await
    }

    pub async fn disallow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DisallowQueryingOptions>>,
    ) -> error::Result<()> {
        self.client
            .disallow_querying(index_name.into(), opts.into())
            .await
    }

    pub async fn freeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<FreezePlanOptions>>,
    ) -> error::Result<()> {
        self.client
            .freeze_plan(index_name.into(), opts.into())
            .await
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<UnfreezePlanOptions>>,
    ) -> error::Result<()> {
        self.client
            .unfreeze_plan(index_name.into(), opts.into())
            .await
    }
}
