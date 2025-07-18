use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::management::search::index::SearchIndex;
use crate::options::search_index_mgmt_options::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetAllIndexesOptions, GetIndexOptions, GetIndexedDocumentsCountOptions,
    PauseIngestOptions, ResumeIngestOptions, UnfreezePlanOptions, UpsertIndexOptions,
};
use couchbase_core::options::search_management;
use couchbase_core::retry::RetryStrategy;
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct SearchIndexMgmtClient {
    backend: SearchIndexMgmtClientBackend,
}

impl SearchIndexMgmtClient {
    pub fn new(backend: SearchIndexMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_index(
        &self,
        index_name: String,
        opts: Option<GetIndexOptions>,
    ) -> error::Result<SearchIndex> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_index(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_index(index_name, opts).await
            }
        }
    }

    pub async fn get_all_indexes(
        &self,
        opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
        }
    }

    pub async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: Option<UpsertIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.upsert_index(index, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.upsert_index(index, opts).await
            }
        }
    }

    pub async fn delete_index(
        &self,
        index_name: String,
        opts: Option<DeleteIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.delete_index(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.delete_index(index_name, opts).await
            }
        }
    }

    pub async fn analyze_document(
        &self,
        index_name: String,
        document: Value,
        opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.analyze_document(index_name, document, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.analyze_document(index_name, document, opts).await
            }
        }
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: String,
        opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_indexed_documents_count(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_indexed_documents_count(index_name, opts).await
            }
        }
    }

    pub async fn pause_ingest(
        &self,
        index_name: String,
        opts: Option<PauseIngestOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.pause_ingest(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.pause_ingest(index_name, opts).await
            }
        }
    }

    pub async fn resume_ingest(
        &self,
        index_name: String,
        opts: Option<ResumeIngestOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.resume_ingest(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.resume_ingest(index_name, opts).await
            }
        }
    }

    pub async fn allow_querying(
        &self,
        index_name: String,
        opts: Option<AllowQueryingOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.allow_querying(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.allow_querying(index_name, opts).await
            }
        }
    }

    pub async fn disallow_querying(
        &self,
        index_name: String,
        opts: Option<DisallowQueryingOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.disallow_querying(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.disallow_querying(index_name, opts).await
            }
        }
    }

    pub async fn freeze_plan(
        &self,
        index_name: String,
        opts: Option<FreezePlanOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.freeze_plan(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.freeze_plan(index_name, opts).await
            }
        }
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: String,
        opts: Option<UnfreezePlanOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.unfreeze_plan(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.unfreeze_plan(index_name, opts).await
            }
        }
    }
}

pub(crate) enum SearchIndexMgmtClientBackend {
    CouchbaseSearchIndexMgmtClientBackend(CouchbaseSearchIndexMgmtClient),
    Couchbase2SearchIndexMgmtClientBackend(Couchbase2SearchIndexMgmtClient),
}

pub(crate) struct SearchIndexKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}
pub(crate) struct CouchbaseSearchIndexMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: SearchIndexKeyspace,
    default_retry_strategy: Arc<dyn RetryStrategy>,
}

impl CouchbaseSearchIndexMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        keyspace: SearchIndexKeyspace,
        default_retry_strategy: Arc<dyn RetryStrategy>,
    ) -> Self {
        Self {
            agent_provider,
            keyspace,
            default_retry_strategy,
        }
    }

    async fn get_index(
        &self,
        index_name: String,
        opts: Option<GetIndexOptions>,
    ) -> error::Result<SearchIndex> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let get_opts = search_management::GetIndexOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let index = agent.get_search_index(&get_opts).await?;
        Ok(index.into())
    }

    async fn get_all_indexes(
        &self,
        opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        let opts = opts.unwrap_or_default();

        let agent = self.agent_provider.get_agent().await;

        let get_all_opts = search_management::GetAllIndexesOptions::new()
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let indexes = agent.get_all_search_indexes(&get_all_opts).await?;

        Ok(indexes.into_iter().map(SearchIndex::from).collect())
    }

    async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: Option<UpsertIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let index = &index.into();
        let upsert_opts = search_management::UpsertIndexOptions::new(index)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.upsert_search_index(&upsert_opts).await?;
        Ok(())
    }

    async fn delete_index(
        &self,
        index_name: String,
        opts: Option<DeleteIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let delete_opts = search_management::DeleteIndexOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.delete_search_index(&delete_opts).await?;
        Ok(())
    }

    async fn analyze_document(
        &self,
        index_name: String,
        document: Value,
        opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let value =
            serde_json::to_vec(&document).map_err(error::Error::encoding_failure_from_serde)?;

        let analyze_opts = search_management::AnalyzeDocumentOptions::new(&index_name, &value)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let analysis = agent.analyze_search_document(&analyze_opts).await?;

        let analysed = serde_json::from_slice(&analysis.analyzed)
            .map_err(error::Error::decoding_failure_from_serde)?;

        Ok(analysed)
    }

    async fn get_indexed_documents_count(
        &self,
        index_name: String,
        opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let count_opts = search_management::GetIndexedDocumentsCountOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        let count = agent
            .get_search_indexed_documents_count(&count_opts)
            .await?;
        Ok(count)
    }

    async fn pause_ingest(
        &self,
        index_name: String,
        opts: Option<PauseIngestOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let pause_opts = search_management::PauseIngestOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.pause_search_index_ingest(&pause_opts).await?;
        Ok(())
    }

    async fn resume_ingest(
        &self,
        index_name: String,
        opts: Option<ResumeIngestOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let resume_opts = search_management::ResumeIngestOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.resume_search_index_ingest(&resume_opts).await?;
        Ok(())
    }

    async fn allow_querying(
        &self,
        index_name: String,
        opts: Option<AllowQueryingOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let allow_opts = search_management::AllowQueryingOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.allow_search_index_querying(&allow_opts).await?;
        Ok(())
    }

    async fn disallow_querying(
        &self,
        index_name: String,
        opts: Option<DisallowQueryingOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let disallow_opts = search_management::DisallowQueryingOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.disallow_search_index_querying(&disallow_opts).await?;
        Ok(())
    }

    async fn freeze_plan(
        &self,
        index_name: String,
        opts: Option<FreezePlanOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let freeze_opts = search_management::FreezePlanOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.freeze_search_index_plan(&freeze_opts).await?;
        Ok(())
    }

    async fn unfreeze_plan(
        &self,
        index_name: String,
        opts: Option<UnfreezePlanOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let unfreeze_opts = search_management::UnfreezePlanOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name)
            .retry_strategy(self.default_retry_strategy.clone());

        agent.unfreeze_search_index_plan(&unfreeze_opts).await?;
        Ok(())
    }
}

pub(crate) struct Couchbase2SearchIndexMgmtClient {}

impl Couchbase2SearchIndexMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn get_index(
        &self,
        _index_name: String,
        _opts: Option<GetIndexOptions>,
    ) -> error::Result<SearchIndex> {
        unimplemented!()
    }

    async fn get_all_indexes(
        &self,
        _opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        unimplemented!()
    }

    async fn upsert_index(
        &self,
        _index: SearchIndex,
        _opts: Option<UpsertIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn delete_index(
        &self,
        _index_name: String,
        _opts: Option<DeleteIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn analyze_document(
        &self,
        _index_name: String,
        _document: Value,
        _opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        unimplemented!()
    }

    async fn get_indexed_documents_count(
        &self,
        _index_name: String,
        _opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        unimplemented!()
    }

    async fn pause_ingest(
        &self,
        _index_name: String,
        _opts: Option<PauseIngestOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn resume_ingest(
        &self,
        _index_name: String,
        _opts: Option<ResumeIngestOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn allow_querying(
        &self,
        _index_name: String,
        _opts: Option<AllowQueryingOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn disallow_querying(
        &self,
        _index_name: String,
        _opts: Option<DisallowQueryingOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn freeze_plan(
        &self,
        _index_name: String,
        _opts: Option<FreezePlanOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn unfreeze_plan(
        &self,
        _index_name: String,
        _opts: Option<UnfreezePlanOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
