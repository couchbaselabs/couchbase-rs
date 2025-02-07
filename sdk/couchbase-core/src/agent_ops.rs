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
    CreateCollectionOptions, CreateScopeOptions, DeleteCollectionOptions, DeleteScopeOptions,
    EnsureManifestOptions, GetCollectionManifestOptions, UpdateCollectionOptions,
};
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
use crate::searchoptions::SearchOptions;
use crate::tracingcomponent::ClusterLabels;

impl Agent {
    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        self.inner.bucket_features().await
    }

    pub fn cluster_labels(&self) -> Option<ClusterLabels> {
        self.inner.tracing.get_cluster_labels()
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
}
