use crate::agent::Agent;
use crate::analyticscomponent::AnalyticsResultStream;
use crate::analyticsoptions::AnalyticsOptions;
use crate::cbconfig::CollectionManifest;
use crate::crudoptions::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetCollectionIdOptions, GetMetaOptions, GetOptions, IncrementOptions,
    LookupInOptions, MutateInOptions, PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions,
    UpsertOptions,
};
use crate::crudresults::{
    AddResult, AppendResult, DecrementResult, DeleteResult, GetAndLockResult, GetAndTouchResult,
    GetCollectionIdResult, GetMetaResult, GetResult, IncrementResult, LookupInResult,
    MutateInResult, PrependResult, ReplaceResult, TouchResult, UnlockResult, UpsertResult,
};
use crate::error::Result;
use crate::features::BucketFeature;
use crate::mgmtoptions::{
    CreateBucketOptions, CreateCollectionOptions, CreateScopeOptions, DeleteBucketOptions,
    DeleteCollectionOptions, DeleteScopeOptions, EnsureBucketOptions, EnsureManifestOptions,
    FlushBucketOptions, GetAllBucketsOptions, GetBucketOptions, GetCollectionManifestOptions,
    UpdateBucketOptions, UpdateCollectionOptions,
};
use crate::mgmtx::bucket_settings::BucketDef;
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use crate::querycomponent::QueryResultStream;
use crate::queryoptions::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, GetAllIndexesOptions, QueryOptions, WatchIndexesOptions,
};
use crate::queryx::index::Index;
use crate::searchcomponent::SearchResultStream;
use crate::searchmgmt_options::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetIndexOptions, GetIndexedDocumentsCountOptions, PauseIngestOptions,
    ResumeIngestOptions, UnfreezePlanOptions, UpsertIndexOptions,
};
use crate::searchoptions::SearchOptions;
use crate::searchx::document_analysis::DocumentAnalysis;
use crate::{searchmgmt_options, searchx};

impl Agent {
    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        self.inner.bucket_features().await
    }

    pub async fn upsert(&self, opts: UpsertOptions<'_>) -> Result<UpsertResult> {
        self.inner.crud.upsert(opts).await
    }

    pub async fn get(&self, opts: GetOptions<'_>) -> Result<GetResult> {
        self.inner.crud.get(opts).await
    }

    pub async fn get_meta(&self, opts: GetMetaOptions<'_>) -> Result<GetMetaResult> {
        self.inner.crud.get_meta(opts).await
    }

    pub async fn delete(&self, opts: DeleteOptions<'_>) -> Result<DeleteResult> {
        self.inner.crud.delete(opts).await
    }

    pub async fn get_and_lock(&self, opts: GetAndLockOptions<'_>) -> Result<GetAndLockResult> {
        self.inner.crud.get_and_lock(opts).await
    }

    pub async fn get_and_touch(&self, opts: GetAndTouchOptions<'_>) -> Result<GetAndTouchResult> {
        self.inner.crud.get_and_touch(opts).await
    }

    pub async fn unlock(&self, opts: UnlockOptions<'_>) -> Result<UnlockResult> {
        self.inner.crud.unlock(opts).await
    }

    pub async fn touch(&self, opts: TouchOptions<'_>) -> Result<TouchResult> {
        self.inner.crud.touch(opts).await
    }

    pub async fn add(&self, opts: AddOptions<'_>) -> Result<AddResult> {
        self.inner.crud.add(opts).await
    }

    pub async fn replace(&self, opts: ReplaceOptions<'_>) -> Result<ReplaceResult> {
        self.inner.crud.replace(opts).await
    }

    pub async fn append(&self, opts: AppendOptions<'_>) -> Result<AppendResult> {
        self.inner.crud.append(opts).await
    }

    pub async fn prepend(&self, opts: PrependOptions<'_>) -> Result<PrependResult> {
        self.inner.crud.prepend(opts).await
    }

    pub async fn increment(&self, opts: IncrementOptions<'_>) -> Result<IncrementResult> {
        self.inner.crud.increment(opts).await
    }

    pub async fn decrement(&self, opts: DecrementOptions<'_>) -> Result<DecrementResult> {
        self.inner.crud.decrement(opts).await
    }

    pub async fn get_collection_id(
        &self,
        opts: GetCollectionIdOptions<'_>,
    ) -> Result<GetCollectionIdResult> {
        self.inner.crud.get_collection_id(opts).await
    }

    pub async fn lookup_in(&self, opts: LookupInOptions<'_>) -> Result<LookupInResult> {
        self.inner.crud.lookup_in(opts).await
    }

    pub async fn mutate_in(&self, opts: MutateInOptions<'_>) -> Result<MutateInResult> {
        self.inner.crud.mutate_in(opts).await
    }

    pub async fn query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        self.inner.query.query(opts).await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        self.inner.query.prepared_query(opts).await
    }

    pub async fn get_all_indexes(&self, opts: &GetAllIndexesOptions<'_>) -> Result<Vec<Index>> {
        self.inner.query.get_all_indexes(opts).await
    }

    pub async fn create_primary_index(&self, opts: &CreatePrimaryIndexOptions<'_>) -> Result<()> {
        self.inner.query.create_primary_index(opts).await
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> Result<()> {
        self.inner.query.create_index(opts).await
    }

    pub async fn drop_primary_index(&self, opts: &DropPrimaryIndexOptions<'_>) -> Result<()> {
        self.inner.query.drop_primary_index(opts).await
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> Result<()> {
        self.inner.query.drop_index(opts).await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> Result<()> {
        self.inner.query.build_deferred_indexes(opts).await
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> Result<()> {
        self.inner.query.watch_indexes(opts).await
    }

    pub async fn search(&self, opts: SearchOptions) -> Result<SearchResultStream> {
        self.inner.search.query(opts).await
    }

    pub async fn get_search_index(
        &self,
        opts: &GetIndexOptions<'_>,
    ) -> Result<searchx::index::Index> {
        self.inner.search.get_index(opts).await
    }

    pub async fn upsert_search_index(&self, opts: &UpsertIndexOptions<'_>) -> Result<()> {
        self.inner.search.upsert_index(opts).await
    }

    pub async fn delete_search_index(&self, opts: &DeleteIndexOptions<'_>) -> Result<()> {
        self.inner.search.delete_index(opts).await
    }

    pub async fn get_all_search_indexes(
        &self,
        opts: &searchmgmt_options::GetAllIndexesOptions<'_>,
    ) -> Result<Vec<searchx::index::Index>> {
        self.inner.search.get_all_indexes(opts).await
    }

    pub async fn analyze_search_document(
        &self,
        opts: &AnalyzeDocumentOptions<'_>,
    ) -> Result<DocumentAnalysis> {
        self.inner.search.analyze_document(opts).await
    }

    pub async fn get_search_indexed_documents_count(
        &self,
        opts: &GetIndexedDocumentsCountOptions<'_>,
    ) -> Result<u64> {
        self.inner.search.get_indexed_documents_count(opts).await
    }

    pub async fn pause_search_index_ingest(&self, opts: &PauseIngestOptions<'_>) -> Result<()> {
        self.inner.search.pause_ingest(opts).await
    }

    pub async fn resume_search_index_ingest(&self, opts: &ResumeIngestOptions<'_>) -> Result<()> {
        self.inner.search.resume_ingest(opts).await
    }

    pub async fn allow_search_index_querying(&self, opts: &AllowQueryingOptions<'_>) -> Result<()> {
        self.inner.search.allow_querying(opts).await
    }

    pub async fn disallow_search_index_querying(
        &self,
        opts: &DisallowQueryingOptions<'_>,
    ) -> Result<()> {
        self.inner.search.disallow_querying(opts).await
    }

    pub async fn freeze_search_index_plan(&self, opts: &FreezePlanOptions<'_>) -> Result<()> {
        self.inner.search.freeze_plan(opts).await
    }

    pub async fn unfreeze_search_index_plan(&self, opts: &UnfreezePlanOptions<'_>) -> Result<()> {
        self.inner.search.unfreeze_plan(opts).await
    }

    pub async fn analytics(&self, opts: AnalyticsOptions<'_>) -> Result<AnalyticsResultStream> {
        self.inner.analytics.query(opts).await
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> Result<CollectionManifest> {
        self.inner.mgmt.get_collection_manifest(opts).await
    }

    pub async fn create_scope(&self, opts: &CreateScopeOptions<'_>) -> Result<CreateScopeResponse> {
        self.inner.mgmt.create_scope(opts).await
    }

    pub async fn delete_scope(&self, opts: &DeleteScopeOptions<'_>) -> Result<DeleteScopeResponse> {
        self.inner.mgmt.delete_scope(opts).await
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> Result<CreateCollectionResponse> {
        self.inner.mgmt.create_collection(opts).await
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> Result<DeleteCollectionResponse> {
        self.inner.mgmt.delete_collection(opts).await
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> Result<UpdateCollectionResponse> {
        self.inner.mgmt.update_collection(opts).await
    }

    pub async fn ensure_manifest(&self, opts: &EnsureManifestOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_manifest(opts).await
    }

    pub async fn get_all_buckets(&self, opts: &GetAllBucketsOptions<'_>) -> Result<Vec<BucketDef>> {
        self.inner.mgmt.get_all_buckets(opts).await
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> Result<BucketDef> {
        self.inner.mgmt.get_bucket(opts).await
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.create_bucket(opts).await
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.update_bucket(opts).await
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.delete_bucket(opts).await
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.flush_bucket(opts).await
    }

    pub async fn ensure_bucket(&self, opts: &EnsureBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_bucket(opts).await
    }

    pub async fn ensure_search_index(
        &self,
        opts: &searchmgmt_options::EnsureIndexOptions<'_>,
    ) -> Result<()> {
        self.inner.search.ensure_index(opts).await
    }
}
