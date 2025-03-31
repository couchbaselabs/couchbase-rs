use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::management::search::index::SearchIndex;
use crate::options::search_index_mgmt_options::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetAllIndexesOptions, GetIndexOptions, GetIndexedDocumentsCountOptions,
    PauseIngestOptions, ResumeIngestOptions, UnfreezePlanOptions, UpsertIndexOptions,
};
use couchbase_core::searchmgmt_options;
use serde_json::Value;

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
}

impl CouchbaseSearchIndexMgmtClient {
    pub fn new(agent_provider: CouchbaseAgentProvider, keyspace: SearchIndexKeyspace) -> Self {
        Self {
            agent_provider,
            keyspace,
        }
    }

    async fn get_index(
        &self,
        index_name: String,
        opts: Option<GetIndexOptions>,
    ) -> error::Result<SearchIndex> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let mut get_opts = searchmgmt_options::GetIndexOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            get_opts = get_opts.retry_strategy(retry_strategy);
        }

        let index = agent.get_search_index(&get_opts).await?;
        Ok(index.into())
    }

    async fn get_all_indexes(
        &self,
        opts: Option<GetAllIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        let opts = opts.unwrap_or_default();

        let agent = self.agent_provider.get_agent().await;

        let mut get_all_opts = searchmgmt_options::GetAllIndexesOptions::new()
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            get_all_opts = get_all_opts.retry_strategy(retry_strategy);
        }

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
        let mut upsert_opts = searchmgmt_options::UpsertIndexOptions::new(index)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            upsert_opts = upsert_opts.retry_strategy(retry_strategy);
        }

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

        let mut delete_opts = searchmgmt_options::DeleteIndexOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            delete_opts = delete_opts.retry_strategy(retry_strategy);
        }

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
            serde_json::to_vec(&document).map_err(|e| error::Error { msg: e.to_string() })?;

        let mut analyze_opts = searchmgmt_options::AnalyzeDocumentOptions::new(&index_name, &value)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            analyze_opts = analyze_opts.retry_strategy(retry_strategy);
        }

        let analysis = agent.analyze_search_document(&analyze_opts).await?;

        let analysed = serde_json::from_slice(&analysis.analyzed)
            .map_err(|e| error::Error { msg: e.to_string() })?;

        Ok(analysed)
    }

    async fn get_indexed_documents_count(
        &self,
        index_name: String,
        opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        let opts = opts.unwrap_or_default();
        let agent = self.agent_provider.get_agent().await;

        let mut count_opts = searchmgmt_options::GetIndexedDocumentsCountOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            count_opts = count_opts.retry_strategy(retry_strategy);
        }

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

        let mut pause_opts = searchmgmt_options::PauseIngestOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            pause_opts = pause_opts.retry_strategy(retry_strategy);
        }

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

        let mut resume_opts = searchmgmt_options::ResumeIngestOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            resume_opts = resume_opts.retry_strategy(retry_strategy);
        }

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

        let mut allow_opts = searchmgmt_options::AllowQueryingOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            allow_opts = allow_opts.retry_strategy(retry_strategy);
        }

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

        let mut disallow_opts = searchmgmt_options::DisallowQueryingOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            disallow_opts = disallow_opts.retry_strategy(retry_strategy);
        }

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

        let mut freeze_opts = searchmgmt_options::FreezePlanOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            freeze_opts = freeze_opts.retry_strategy(retry_strategy);
        }

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

        let mut unfreeze_opts = searchmgmt_options::UnfreezePlanOptions::new(&index_name)
            .bucket_name(&self.keyspace.bucket_name)
            .scope_name(&self.keyspace.scope_name);
        if let Some(retry_strategy) = opts.retry_strategy {
            unfreeze_opts = unfreeze_opts.retry_strategy(retry_strategy);
        }

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
