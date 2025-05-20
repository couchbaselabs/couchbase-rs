use crate::common::helpers::{
    run_with_std_analytics_deadline, run_with_std_ensure_deadline, run_with_std_mgmt_deadline,
    run_with_std_search_deadline,
};
use crate::common::helpers::{run_with_std_kv_deadline, run_with_std_query_deadline};
use crate::common::node_version::NodeVersion;
use crate::common::test_config::TestSetupConfig;
use couchbase_core::agent::Agent;
use couchbase_core::analyticscomponent::AnalyticsResultStream;
use couchbase_core::analyticsoptions::AnalyticsOptions;
use couchbase_core::cbconfig::CollectionManifest;
use couchbase_core::crudoptions::*;
use couchbase_core::crudresults::*;
use couchbase_core::error::Result;
use couchbase_core::features::BucketFeature;
use couchbase_core::mgmtoptions::*;
use couchbase_core::mgmtx::bucket_settings::BucketDef;
use couchbase_core::mgmtx::responses::*;
use couchbase_core::mgmtx::user::{Group, RoleAndDescription, UserAndMetadata};
use couchbase_core::querycomponent::QueryResultStream;
use couchbase_core::queryoptions::*;
use couchbase_core::queryx::index::Index;
use couchbase_core::searchcomponent::SearchResultStream;
use couchbase_core::searchmgmt_options::*;
use couchbase_core::searchx::document_analysis::DocumentAnalysis;
use couchbase_core::{searchmgmt_options, searchoptions::SearchOptions, searchx};
use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub struct TestAgent {
    pub test_setup_config: TestSetupConfig,
    pub cluster_version: NodeVersion,
    agent: Agent,
}

impl TestAgent {
    pub fn new(
        test_setup_config: TestSetupConfig,
        cluster_version: NodeVersion,
        agent: Agent,
    ) -> Self {
        Self {
            test_setup_config,
            cluster_version,
            agent,
        }
    }
}

impl Deref for TestAgent {
    type Target = Agent;

    fn deref(&self) -> &Self::Target {
        &self.agent
    }
}

impl DerefMut for TestAgent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.agent
    }
}

impl TestAgent {
    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        self.agent.bucket_features().await
    }

    pub async fn upsert(&self, opts: UpsertOptions<'_>) -> Result<UpsertResult> {
        run_with_std_kv_deadline(self.agent.upsert(opts)).await
    }

    pub async fn get(&self, opts: GetOptions<'_>) -> Result<GetResult> {
        run_with_std_kv_deadline(self.agent.get(opts)).await
    }

    pub async fn get_meta(&self, opts: GetMetaOptions<'_>) -> Result<GetMetaResult> {
        run_with_std_kv_deadline(self.agent.get_meta(opts)).await
    }

    pub async fn delete(&self, opts: DeleteOptions<'_>) -> Result<DeleteResult> {
        run_with_std_kv_deadline(self.agent.delete(opts)).await
    }

    pub async fn get_and_lock(&self, opts: GetAndLockOptions<'_>) -> Result<GetAndLockResult> {
        run_with_std_kv_deadline(self.agent.get_and_lock(opts)).await
    }

    pub async fn get_and_touch(&self, opts: GetAndTouchOptions<'_>) -> Result<GetAndTouchResult> {
        run_with_std_kv_deadline(self.agent.get_and_touch(opts)).await
    }

    pub async fn unlock(&self, opts: UnlockOptions<'_>) -> Result<UnlockResult> {
        run_with_std_kv_deadline(self.agent.unlock(opts)).await
    }

    pub async fn touch(&self, opts: TouchOptions<'_>) -> Result<TouchResult> {
        run_with_std_kv_deadline(self.agent.touch(opts)).await
    }

    pub async fn add(&self, opts: AddOptions<'_>) -> Result<AddResult> {
        run_with_std_kv_deadline(self.agent.add(opts)).await
    }

    pub async fn replace(&self, opts: ReplaceOptions<'_>) -> Result<ReplaceResult> {
        run_with_std_kv_deadline(self.agent.replace(opts)).await
    }

    pub async fn append(&self, opts: AppendOptions<'_>) -> Result<AppendResult> {
        run_with_std_kv_deadline(self.agent.append(opts)).await
    }

    pub async fn prepend(&self, opts: PrependOptions<'_>) -> Result<PrependResult> {
        run_with_std_kv_deadline(self.agent.prepend(opts)).await
    }

    pub async fn increment(&self, opts: IncrementOptions<'_>) -> Result<IncrementResult> {
        run_with_std_kv_deadline(self.agent.increment(opts)).await
    }

    pub async fn decrement(&self, opts: DecrementOptions<'_>) -> Result<DecrementResult> {
        run_with_std_kv_deadline(self.agent.decrement(opts)).await
    }

    pub async fn get_collection_id(
        &self,
        opts: GetCollectionIdOptions<'_>,
    ) -> Result<GetCollectionIdResult> {
        run_with_std_kv_deadline(self.agent.get_collection_id(opts)).await
    }

    pub async fn lookup_in(&self, opts: LookupInOptions<'_>) -> Result<LookupInResult> {
        run_with_std_kv_deadline(self.agent.lookup_in(opts)).await
    }

    pub async fn mutate_in(&self, opts: MutateInOptions<'_>) -> Result<MutateInResult> {
        run_with_std_kv_deadline(self.agent.mutate_in(opts)).await
    }

    pub async fn query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        run_with_std_query_deadline(self.agent.query(opts)).await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        run_with_std_query_deadline(self.agent.prepared_query(opts)).await
    }

    pub async fn get_all_indexes(
        &self,
        opts: &couchbase_core::queryoptions::GetAllIndexesOptions<'_>,
    ) -> Result<Vec<Index>> {
        run_with_std_query_deadline(self.agent.get_all_indexes(opts)).await
    }

    pub async fn create_primary_index(&self, opts: &CreatePrimaryIndexOptions<'_>) -> Result<()> {
        run_with_std_query_deadline(self.agent.create_primary_index(opts)).await
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> Result<()> {
        run_with_std_query_deadline(self.agent.create_index(opts)).await
    }

    pub async fn drop_primary_index(&self, opts: &DropPrimaryIndexOptions<'_>) -> Result<()> {
        run_with_std_query_deadline(self.agent.drop_primary_index(opts)).await
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> Result<()> {
        run_with_std_query_deadline(self.agent.drop_index(opts)).await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> Result<()> {
        run_with_std_query_deadline(self.agent.build_deferred_indexes(opts)).await
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> Result<()> {
        run_with_std_query_deadline(self.agent.watch_indexes(opts)).await
    }

    pub async fn search(&self, opts: SearchOptions) -> Result<SearchResultStream> {
        run_with_std_search_deadline(self.agent.search(opts)).await
    }

    pub async fn get_search_index(
        &self,
        opts: &GetIndexOptions<'_>,
    ) -> Result<searchx::index::Index> {
        run_with_std_mgmt_deadline(self.agent.get_search_index(opts)).await
    }

    pub async fn upsert_search_index(&self, opts: &UpsertIndexOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.upsert_search_index(opts)).await
    }

    pub async fn delete_search_index(&self, opts: &DeleteIndexOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.delete_search_index(opts)).await
    }

    pub async fn get_all_search_indexes(
        &self,
        opts: &searchmgmt_options::GetAllIndexesOptions<'_>,
    ) -> Result<Vec<searchx::index::Index>> {
        run_with_std_mgmt_deadline(self.agent.get_all_search_indexes(opts)).await
    }

    pub async fn analyze_search_document(
        &self,
        opts: &AnalyzeDocumentOptions<'_>,
    ) -> Result<DocumentAnalysis> {
        run_with_std_mgmt_deadline(self.agent.analyze_search_document(opts)).await
    }

    pub async fn get_search_indexed_documents_count(
        &self,
        opts: &GetIndexedDocumentsCountOptions<'_>,
    ) -> Result<u64> {
        run_with_std_mgmt_deadline(self.agent.get_search_indexed_documents_count(opts)).await
    }

    pub async fn pause_search_index_ingest(&self, opts: &PauseIngestOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.pause_search_index_ingest(opts)).await
    }

    pub async fn resume_search_index_ingest(&self, opts: &ResumeIngestOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.resume_search_index_ingest(opts)).await
    }

    pub async fn allow_search_index_querying(&self, opts: &AllowQueryingOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.allow_search_index_querying(opts)).await
    }

    pub async fn disallow_search_index_querying(
        &self,
        opts: &DisallowQueryingOptions<'_>,
    ) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.disallow_search_index_querying(opts)).await
    }

    pub async fn freeze_search_index_plan(&self, opts: &FreezePlanOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.freeze_search_index_plan(opts)).await
    }

    pub async fn unfreeze_search_index_plan(&self, opts: &UnfreezePlanOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.unfreeze_search_index_plan(opts)).await
    }

    pub async fn analytics(&self, opts: AnalyticsOptions<'_>) -> Result<AnalyticsResultStream> {
        run_with_std_analytics_deadline(self.agent.analytics(opts)).await
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> Result<CollectionManifest> {
        self.agent.get_collection_manifest(opts).await
    }

    pub async fn create_scope(&self, opts: &CreateScopeOptions<'_>) -> Result<CreateScopeResponse> {
        run_with_std_mgmt_deadline(self.agent.create_scope(opts)).await
    }

    pub async fn delete_scope(&self, opts: &DeleteScopeOptions<'_>) -> Result<DeleteScopeResponse> {
        run_with_std_mgmt_deadline(self.agent.delete_scope(opts)).await
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> Result<CreateCollectionResponse> {
        run_with_std_mgmt_deadline(self.agent.create_collection(opts)).await
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> Result<DeleteCollectionResponse> {
        run_with_std_mgmt_deadline(self.agent.delete_collection(opts)).await
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> Result<UpdateCollectionResponse> {
        run_with_std_mgmt_deadline(self.agent.update_collection(opts)).await
    }

    pub async fn ensure_manifest(&self, opts: &EnsureManifestOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.ensure_manifest(opts)).await
    }

    pub async fn get_all_buckets(&self, opts: &GetAllBucketsOptions<'_>) -> Result<Vec<BucketDef>> {
        run_with_std_mgmt_deadline(self.agent.get_all_buckets(opts)).await
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> Result<BucketDef> {
        run_with_std_mgmt_deadline(self.agent.get_bucket(opts)).await
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.create_bucket(opts)).await
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.update_bucket(opts)).await
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.delete_bucket(opts)).await
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.flush_bucket(opts)).await
    }

    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> Result<UserAndMetadata> {
        run_with_std_mgmt_deadline(self.agent.get_user(opts)).await
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> Result<Vec<UserAndMetadata>> {
        run_with_std_mgmt_deadline(self.agent.get_all_users(opts)).await
    }

    pub async fn upsert_user(&self, opts: &UpsertUserOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.upsert_user(opts)).await
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.delete_user(opts)).await
    }

    pub async fn get_roles(&self, opts: &GetRolesOptions<'_>) -> Result<Vec<RoleAndDescription>> {
        run_with_std_mgmt_deadline(self.agent.get_roles(opts)).await
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> Result<Group> {
        run_with_std_mgmt_deadline(self.agent.get_group(opts)).await
    }

    pub async fn get_all_groups(&self, opts: &GetAllGroupsOptions<'_>) -> Result<Vec<Group>> {
        run_with_std_mgmt_deadline(self.agent.get_all_groups(opts)).await
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.upsert_group(opts)).await
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.delete_group(opts)).await
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> Result<()> {
        run_with_std_mgmt_deadline(self.agent.change_password(opts)).await
    }

    pub async fn ensure_user(&self, opts: &EnsureUserOptions<'_>) -> Result<()> {
        run_with_std_ensure_deadline(self.agent.ensure_user(opts)).await
    }

    pub async fn ensure_group(&self, opts: &EnsureGroupOptions<'_>) -> Result<()> {
        run_with_std_ensure_deadline(self.agent.ensure_group(opts)).await
    }

    pub async fn ensure_bucket(&self, opts: &EnsureBucketOptions<'_>) -> Result<()> {
        run_with_std_ensure_deadline(self.agent.ensure_bucket(opts)).await
    }

    pub async fn ensure_search_index(&self, opts: &EnsureIndexOptions<'_>) -> Result<()> {
        run_with_std_ensure_deadline(self.agent.ensure_search_index(opts)).await
    }
}
