use couchbase::error;
use couchbase::management::search::index::*;
use couchbase::management::search::search_index_manager::SearchIndexManager;
use couchbase::options::search_index_mgmt_options::*;
use serde_json::Value;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct TestSearchIndexManager {
    inner: SearchIndexManager,
}

impl TestSearchIndexManager {
    pub fn new(inner: SearchIndexManager) -> Self {
        Self { inner }
    }

    pub async fn get_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetIndexOptions>>,
    ) -> error::Result<SearchIndex> {
        timeout(
            Duration::from_secs(20),
            self.inner.get_index(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn get_all_indexes(
        &self,
        opts: impl Into<Option<GetAllIndexesOptions>>,
    ) -> error::Result<Vec<SearchIndex>> {
        timeout(Duration::from_secs(20), self.inner.get_all_indexes(opts))
            .await
            .unwrap()
    }

    pub async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: impl Into<Option<UpsertIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.upsert_index(index, opts),
        )
        .await
        .unwrap()
    }

    pub async fn drop_index(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DeleteIndexOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.drop_index(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn analyze_document(
        &self,
        index_name: impl Into<String>,
        document: Value,
        opts: impl Into<Option<AnalyzeDocumentOptions>>,
    ) -> error::Result<Value> {
        timeout(
            Duration::from_secs(20),
            self.inner.analyze_document(index_name, document, opts),
        )
        .await
        .unwrap()
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<GetIndexedDocumentsCountOptions>>,
    ) -> error::Result<u64> {
        timeout(
            Duration::from_secs(20),
            self.inner.get_indexed_documents_count(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn pause_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<PauseIngestOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.pause_ingest(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn resume_ingest(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<ResumeIngestOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.resume_ingest(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn allow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<AllowQueryingOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.allow_querying(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn disallow_querying(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<DisallowQueryingOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.disallow_querying(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn freeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<FreezePlanOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.freeze_plan(index_name, opts),
        )
        .await
        .unwrap()
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: impl Into<String>,
        opts: impl Into<Option<UnfreezePlanOptions>>,
    ) -> error::Result<()> {
        timeout(
            Duration::from_secs(20),
            self.inner.unfreeze_plan(index_name, opts),
        )
        .await
        .unwrap()
    }
}
