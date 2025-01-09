use crate::agent::Agent;
use crate::analyticscomponent::AnalyticsResultStream;
use crate::analyticsoptions::AnalyticsOptions;
use crate::cbconfig::CollectionManifest;
use crate::crudoptions::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetMetaOptions, GetOptions, IncrementOptions, LookupInOptions,
    MutateInOptions, PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions, UpsertOptions,
};
use crate::crudresults::{
    AddResult, AppendResult, DecrementResult, DeleteResult, GetAndLockResult, GetAndTouchResult,
    GetMetaResult, GetResult, IncrementResult, LookupInResult, MutateInResult, PrependResult,
    ReplaceResult, TouchResult, UnlockResult, UpsertResult,
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
use crate::queryoptions::QueryOptions;
use crate::searchcomponent::SearchResultStream;
use crate::searchoptions::SearchOptions;

impl Agent {
    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        self.inner.bucket_features().await
    }

    pub async fn upsert<'a>(&self, opts: UpsertOptions<'a>) -> Result<UpsertResult> {
        self.inner.crud.upsert(opts).await
    }

    pub async fn get<'a>(&self, opts: GetOptions<'a>) -> Result<GetResult> {
        self.inner.crud.get(opts).await
    }

    pub async fn get_meta<'a>(&self, opts: GetMetaOptions<'a>) -> Result<GetMetaResult> {
        self.inner.crud.get_meta(opts).await
    }

    pub async fn delete<'a>(&self, opts: DeleteOptions<'a>) -> Result<DeleteResult> {
        self.inner.crud.delete(opts).await
    }

    pub async fn get_and_lock<'a>(&self, opts: GetAndLockOptions<'a>) -> Result<GetAndLockResult> {
        self.inner.crud.get_and_lock(opts).await
    }

    pub async fn get_and_touch<'a>(
        &self,
        opts: GetAndTouchOptions<'a>,
    ) -> Result<GetAndTouchResult> {
        self.inner.crud.get_and_touch(opts).await
    }

    pub async fn unlock<'a>(&self, opts: UnlockOptions<'a>) -> Result<UnlockResult> {
        self.inner.crud.unlock(opts).await
    }

    pub async fn touch<'a>(&self, opts: TouchOptions<'a>) -> Result<TouchResult> {
        self.inner.crud.touch(opts).await
    }

    pub async fn add<'a>(&self, opts: AddOptions<'a>) -> Result<AddResult> {
        self.inner.crud.add(opts).await
    }

    pub async fn replace<'a>(&self, opts: ReplaceOptions<'a>) -> Result<ReplaceResult> {
        self.inner.crud.replace(opts).await
    }

    pub async fn append<'a>(&self, opts: AppendOptions<'a>) -> Result<AppendResult> {
        self.inner.crud.append(opts).await
    }

    pub async fn prepend<'a>(&self, opts: PrependOptions<'a>) -> Result<PrependResult> {
        self.inner.crud.prepend(opts).await
    }

    pub async fn increment<'a>(&self, opts: IncrementOptions<'a>) -> Result<IncrementResult> {
        self.inner.crud.increment(opts).await
    }

    pub async fn decrement<'a>(&self, opts: DecrementOptions<'a>) -> Result<DecrementResult> {
        self.inner.crud.decrement(opts).await
    }

    pub async fn lookup_in<'a>(&self, opts: LookupInOptions<'a>) -> Result<LookupInResult> {
        self.inner.crud.lookup_in(opts).await
    }

    pub async fn mutate_in<'a>(&self, opts: MutateInOptions<'a>) -> Result<MutateInResult> {
        self.inner.crud.mutate_in(opts).await
    }

    pub async fn query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        self.inner.query.query(opts).await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        self.inner.query.prepared_query(opts).await
    }

    pub async fn search(&self, opts: SearchOptions) -> Result<SearchResultStream> {
        self.inner.search.query(opts).await
    }

    pub async fn analytics<'a>(&self, opts: AnalyticsOptions<'a>) -> Result<AnalyticsResultStream> {
        self.inner.analytics.query(opts).await
    }

    pub async fn get_collection_manifest<'a>(
        &self,
        opts: &GetCollectionManifestOptions<'a>,
    ) -> Result<CollectionManifest> {
        self.inner.mgmt.get_collection_manifest(opts).await
    }

    pub async fn create_scope<'a>(
        &self,
        opts: &CreateScopeOptions<'a>,
    ) -> Result<CreateScopeResponse> {
        self.inner.mgmt.create_scope(opts).await
    }

    pub async fn delete_scope<'a>(
        &self,
        opts: &DeleteScopeOptions<'a>,
    ) -> Result<DeleteScopeResponse> {
        self.inner.mgmt.delete_scope(opts).await
    }

    pub async fn create_collection<'a>(
        &self,
        opts: &CreateCollectionOptions<'a>,
    ) -> Result<CreateCollectionResponse> {
        self.inner.mgmt.create_collection(opts).await
    }

    pub async fn delete_collection<'a>(
        &self,
        opts: &DeleteCollectionOptions<'a>,
    ) -> Result<DeleteCollectionResponse> {
        self.inner.mgmt.delete_collection(opts).await
    }

    pub async fn update_collection<'a>(
        &self,
        opts: &UpdateCollectionOptions<'a>,
    ) -> Result<UpdateCollectionResponse> {
        self.inner.mgmt.update_collection(opts).await
    }

    pub async fn ensure_manifest<'a>(&self, opts: &EnsureManifestOptions<'a>) -> Result<()> {
        self.inner.mgmt.ensure_manifest(opts).await
    }
}
