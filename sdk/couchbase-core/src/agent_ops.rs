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
use crate::agent::{Agent, AgentInner};
use crate::cbconfig::{CollectionManifest, FullBucketConfig, FullClusterConfig};
use crate::clusterlabels::ClusterLabels;
use crate::create_span;
use crate::error::Result;
use crate::features::BucketFeature;
use crate::mgmtx::bucket_settings::BucketDef;
use crate::mgmtx::mgmt::AutoFailoverSettings;
use crate::mgmtx::mgmt_query::IndexStatus;
use crate::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
    UpdateCollectionResponse,
};
use crate::mgmtx::user::{Group, RoleAndDescription, UserAndMetadata};
use crate::options::analytics::{AnalyticsOptions, GetPendingMutationsOptions};
use crate::options::crud::{
    AddOptions, AppendOptions, DecrementOptions, DeleteOptions, GetAndLockOptions,
    GetAndTouchOptions, GetCollectionIdOptions, GetMetaOptions, GetOptions, IncrementOptions,
    LookupInOptions, MutateInOptions, PrependOptions, ReplaceOptions, TouchOptions, UnlockOptions,
    UpsertOptions,
};
use crate::options::diagnostics::DiagnosticsOptions;
use crate::options::management::{
    ChangePasswordOptions, CreateBucketOptions, CreateCollectionOptions, CreateScopeOptions,
    DeleteBucketOptions, DeleteCollectionOptions, DeleteGroupOptions, DeleteScopeOptions,
    DeleteUserOptions, EnsureBucketOptions, EnsureGroupOptions, EnsureManifestOptions,
    EnsureUserOptions, FlushBucketOptions, GetAllBucketsOptions, GetAllGroupsOptions,
    GetAllUsersOptions, GetAutoFailoverSettingsOptions, GetBucketOptions, GetBucketStatsOptions,
    GetCollectionManifestOptions, GetFullBucketConfigOptions, GetFullClusterConfigOptions,
    GetGroupOptions, GetRolesOptions, GetUserOptions, IndexStatusOptions, LoadSampleBucketOptions,
    UpdateBucketOptions, UpdateCollectionOptions, UpsertGroupOptions, UpsertUserOptions,
};
use crate::options::ping::PingOptions;
use crate::options::query::{
    BuildDeferredIndexesOptions, CreateIndexOptions, CreatePrimaryIndexOptions, DropIndexOptions,
    DropPrimaryIndexOptions, EnsureIndexOptions, GetAllIndexesOptions, QueryOptions,
    WatchIndexesOptions,
};
use crate::options::search::SearchOptions;
use crate::options::search_management;
use crate::options::search_management::{
    AllowQueryingOptions, AnalyzeDocumentOptions, DeleteIndexOptions, DisallowQueryingOptions,
    FreezePlanOptions, GetIndexOptions, GetIndexedDocumentsCountOptions, PauseIngestOptions,
    ResumeIngestOptions, UnfreezePlanOptions, UpsertIndexOptions,
};
use crate::options::waituntilready::WaitUntilReadyOptions;
use crate::queryx::index::Index;
use crate::results::analytics::AnalyticsResultStream;
use crate::results::diagnostics::DiagnosticsResult;
use crate::results::kv::{
    AddResult, AppendResult, DecrementResult, DeleteResult, GetAndLockResult, GetAndTouchResult,
    GetCollectionIdResult, GetMetaResult, GetResult, IncrementResult, LookupInResult,
    MutateInResult, PrependResult, ReplaceResult, TouchResult, UnlockResult, UpsertResult,
};
use crate::results::pingreport::PingReport;
use crate::results::query::QueryResultStream;
use crate::results::search::SearchResultStream;
use crate::searchx;
use crate::searchx::document_analysis::DocumentAnalysis;
use crate::tracingcomponent::{
    build_keyspace, record_metrics, Keyspace, OperationContext, SpanBuilder,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE, SPAN_ATTRIB_OTEL_KIND_KEY,
};
use futures::Future;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::time::Instant;
use tracing::Instrument;

impl Agent {
    fn begin_operation(
        &self,
        service: Option<&'static str>,
        keyspace: Keyspace,
        mut span: SpanBuilder,
    ) -> OperationContext {
        let operation_name = span.name();
        let cluster_labels = self.inner.tracing.get_cluster_labels();
        let built_span = span
            .with_cluster_labels(&cluster_labels)
            .with_service(service)
            .with_keyspace(&keyspace)
            .build();

        OperationContext::new(
            built_span,
            operation_name,
            service,
            keyspace,
            cluster_labels,
        )
    }

    pub async fn bucket_features(&self) -> Result<Vec<BucketFeature>> {
        self.inner.bucket_features().await
    }

    pub fn cluster_labels(&self) -> Option<ClusterLabels> {
        self.inner.tracing.get_cluster_labels()
    }

    pub async fn upsert(&self, opts: UpsertOptions<'_>) -> Result<UpsertResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("upsert"),
        );
        let result = self.inner.crud.upsert(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }

        self.inner.crud.upsert(opts).await
    }

    pub async fn get(&self, opts: GetOptions<'_>) -> Result<GetResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("get"),
        );
        let result = self.inner.crud.get(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.get(opts).await
    }

    pub async fn get_meta(&self, opts: GetMetaOptions<'_>) -> Result<GetMetaResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("get_meta"),
        );
        let result = self.inner.crud.get_meta(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.get_meta(opts).await
    }

    pub async fn delete(&self, opts: DeleteOptions<'_>) -> Result<DeleteResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("delete").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.delete(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.delete(opts).await
    }

    pub async fn get_and_lock(&self, opts: GetAndLockOptions<'_>) -> Result<GetAndLockResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("get_and_lock"),
        );
        let result = self.inner.crud.get_and_lock(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.get_and_lock(opts).await
    }

    pub async fn get_and_touch(&self, opts: GetAndTouchOptions<'_>) -> Result<GetAndTouchResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("get_and_touch"),
        );
        let result = self.inner.crud.get_and_touch(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.get_and_touch(opts).await
    }

    pub async fn unlock(&self, opts: UnlockOptions<'_>) -> Result<UnlockResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("unlock"),
        );
        let result = self.inner.crud.unlock(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.unlock(opts).await
    }

    pub async fn touch(&self, opts: TouchOptions<'_>) -> Result<TouchResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("touch"),
        );
        let result = self.inner.crud.touch(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.touch(opts).await
    }

    pub async fn add(&self, opts: AddOptions<'_>) -> Result<AddResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("add").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.add(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.add(opts).await
    }

    pub async fn replace(&self, opts: ReplaceOptions<'_>) -> Result<ReplaceResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("replace").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.replace(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.replace(opts).await
    }

    pub async fn append(&self, opts: AppendOptions<'_>) -> Result<AppendResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("append").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.append(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.append(opts).await
    }

    pub async fn prepend(&self, opts: PrependOptions<'_>) -> Result<PrependResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("prepend").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.prepend(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.prepend(opts).await
    }

    pub async fn increment(&self, opts: IncrementOptions<'_>) -> Result<IncrementResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("increment"),
        );
        let result = self.inner.crud.increment(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.increment(opts).await
    }

    pub async fn decrement(&self, opts: DecrementOptions<'_>) -> Result<DecrementResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("decrement"),
        );
        let result = self.inner.crud.decrement(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.decrement(opts).await
    }

    pub async fn get_collection_id(
        &self,
        opts: GetCollectionIdOptions<'_>,
    ) -> Result<GetCollectionIdResult> {
        self.inner.crud.get_collection_id(opts).await
    }

    pub async fn lookup_in(&self, opts: LookupInOptions<'_>) -> Result<LookupInResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("lookup_in"),
        );
        let result = self.inner.crud.lookup_in(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.lookup_in(opts).await
    }

    pub async fn mutate_in(&self, opts: MutateInOptions<'_>) -> Result<MutateInResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_KV),
            build_keyspace(
                        self.get_bucket_name().as_deref(),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("mutate_in").with_durability(opts.durability_level.as_ref()),
        );
        let result = self.inner.crud.mutate_in(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.crud.mutate_in(opts).await
    }

    pub async fn query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            Keyspace::Cluster,
            create_span!("query").with_statement(opts.statement.as_deref().unwrap_or("")),
        );
        let result = self.inner.query.query(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.query(opts).await
    }

    pub async fn prepared_query(&self, opts: QueryOptions) -> Result<QueryResultStream> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            Keyspace::Cluster,
            create_span!("query").with_statement(opts.statement.as_deref().unwrap_or("")),
        );
        let result = self.inner.query.prepared_query(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.prepared_query(opts).await
    }

    pub async fn get_all_indexes(&self, opts: &GetAllIndexesOptions<'_>) -> Result<Vec<Index>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_get_all_indexes"),
        );
        let result = self.inner.query.get_all_indexes(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.get_all_indexes(opts).await
    }

    pub async fn create_primary_index(&self, opts: &CreatePrimaryIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_create_primary_index"),
        );
        let result = self.inner.query.create_primary_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.create_primary_index(opts).await
    }

    pub async fn create_index(&self, opts: &CreateIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_create_index"),
        );
        let result = self.inner.query.create_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.create_index(opts).await
    }

    pub async fn drop_primary_index(&self, opts: &DropPrimaryIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_drop_primary_index"),
        );
        let result = self.inner.query.drop_primary_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.drop_primary_index(opts).await
    }

    pub async fn drop_index(&self, opts: &DropIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_drop_index"),
        );
        let result = self.inner.query.drop_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.drop_index(opts).await
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: &BuildDeferredIndexesOptions<'_>,
    ) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_build_deferred_indexes"),
        );
        let result = self.inner.query.build_deferred_indexes(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.build_deferred_indexes(opts).await
    }

    pub async fn watch_indexes(&self, opts: &WatchIndexesOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_QUERY),
            build_keyspace(
                        Some(opts.bucket_name),
                        opts.scope_name,
                        opts.collection_name,
                    ),
            create_span!("manager_query_watch_indexes"),
        );
        let result = self.inner.query.watch_indexes(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.query.watch_indexes(opts).await
    }

    pub async fn ensure_index(&self, opts: &EnsureIndexOptions<'_>) -> Result<()> {
        self.inner.query.ensure_index(opts).await
    }

    pub async fn search(&self, opts: SearchOptions) -> Result<SearchResultStream> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(
                        opts.bucket_name.as_deref(),
                        opts.scope_name.as_deref(),
                        None,
                    ),
            create_span!("search"),
        );
        let result = self.inner.search.query(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.query(opts).await
    }

    pub async fn get_search_index(
        &self,
        opts: &GetIndexOptions<'_>,
    ) -> Result<searchx::index::Index> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_get_index"),
        );
        let result = self.inner.search.get_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.get_index(opts).await
    }

    pub async fn upsert_search_index(&self, opts: &UpsertIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_upsert_index"),
        );
        let result = self.inner.search.upsert_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.upsert_index(opts).await
    }

    pub async fn delete_search_index(&self, opts: &DeleteIndexOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_drop_index"),
        );
        let result = self.inner.search.delete_index(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.delete_index(opts).await
    }

    pub async fn get_all_search_indexes(
        &self,
        opts: &search_management::GetAllIndexesOptions<'_>,
    ) -> Result<Vec<searchx::index::Index>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_get_all_indexes"),
        );
        let result = self.inner.search.get_all_indexes(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.get_all_indexes(opts).await
    }

    pub async fn analyze_search_document(
        &self,
        opts: &AnalyzeDocumentOptions<'_>,
    ) -> Result<DocumentAnalysis> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_analyze_document"),
        );
        let result = self.inner.search.analyze_document(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.analyze_document(opts).await
    }

    pub async fn get_search_indexed_documents_count(
        &self,
        opts: &GetIndexedDocumentsCountOptions<'_>,
    ) -> Result<u64> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_get_indexed_documents_count"),
        );
        let result = self.inner.search.get_indexed_documents_count(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.get_indexed_documents_count(opts).await
    }

    pub async fn pause_search_index_ingest(&self, opts: &PauseIngestOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_pause_ingest"),
        );
        let result = self.inner.search.pause_ingest(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.pause_ingest(opts).await
    }

    pub async fn resume_search_index_ingest(&self, opts: &ResumeIngestOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_resume_ingest"),
        );
        let result = self.inner.search.resume_ingest(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.resume_ingest(opts).await
    }

    pub async fn allow_search_index_querying(&self, opts: &AllowQueryingOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_allow_querying"),
        );
        let result = self.inner.search.allow_querying(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.allow_querying(opts).await
    }

    pub async fn disallow_search_index_querying(
        &self,
        opts: &DisallowQueryingOptions<'_>,
    ) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_disallow_querying"),
        );
        let result = self.inner.search.disallow_querying(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.disallow_querying(opts).await
    }

    pub async fn freeze_search_index_plan(&self, opts: &FreezePlanOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_freeze_plan"),
        );
        let result = self.inner.search.freeze_plan(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.freeze_plan(opts).await
    }

    pub async fn unfreeze_search_index_plan(&self, opts: &UnfreezePlanOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_SEARCH),
            build_keyspace(opts.bucket_name, opts.scope_name, None),
            create_span!("manager_search_unfreeze_plan"),
        );
        let result = self.inner.search.unfreeze_plan(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.search.unfreeze_plan(opts).await
    }

    pub async fn get_collection_manifest(
        &self,
        opts: &GetCollectionManifestOptions<'_>,
    ) -> Result<CollectionManifest> {
        self.inner.mgmt.get_collection_manifest(opts).await
    }

    pub async fn create_scope(&self, opts: &CreateScopeOptions<'_>) -> Result<CreateScopeResponse> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), Some(opts.scope_name), None),
            create_span!("manager_collections_create_scope"),
        );
        let result = self.inner.mgmt.create_scope(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.create_scope(opts).await
    }

    pub async fn delete_scope(&self, opts: &DeleteScopeOptions<'_>) -> Result<DeleteScopeResponse> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), Some(opts.scope_name), None),
            create_span!("manager_collections_drop_scope"),
        );
        let result = self.inner.mgmt.delete_scope(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.delete_scope(opts).await
    }

    pub async fn create_collection(
        &self,
        opts: &CreateCollectionOptions<'_>,
    ) -> Result<CreateCollectionResponse> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(
                        Some(opts.bucket_name),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("manager_collections_create_collection"),
        );
        let result = self.inner.mgmt.create_collection(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.create_collection(opts).await
    }

    pub async fn delete_collection(
        &self,
        opts: &DeleteCollectionOptions<'_>,
    ) -> Result<DeleteCollectionResponse> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(
                        Some(opts.bucket_name),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("manager_collections_drop_collection"),
        );
        let result = self.inner.mgmt.delete_collection(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.delete_collection(opts).await
    }

    pub async fn update_collection(
        &self,
        opts: &UpdateCollectionOptions<'_>,
    ) -> Result<UpdateCollectionResponse> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(
                        Some(opts.bucket_name),
                        Some(opts.scope_name),
                        Some(opts.collection_name),
                    ),
            create_span!("manager_collections_update_collection"),
        );
        let result = self.inner.mgmt.update_collection(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.update_collection(opts).await
    }

    pub async fn ensure_manifest(&self, opts: &EnsureManifestOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_manifest(opts).await
    }

    pub async fn get_all_buckets(&self, opts: &GetAllBucketsOptions<'_>) -> Result<Vec<BucketDef>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_buckets_get_all_buckets"),
        );
        let result = self.inner.mgmt.get_all_buckets(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_all_buckets(opts).await
    }

    pub async fn get_bucket(&self, opts: &GetBucketOptions<'_>) -> Result<BucketDef> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), None, None),
            create_span!("manager_buckets_get_bucket"),
        );
        let result = self.inner.mgmt.get_bucket(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_bucket(opts).await
    }

    pub async fn create_bucket(&self, opts: &CreateBucketOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), None, None),
            create_span!("manager_buckets_create_bucket"),
        );
        let result = self.inner.mgmt.create_bucket(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.create_bucket(opts).await
    }

    pub async fn update_bucket(&self, opts: &UpdateBucketOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), None, None),
            create_span!("manager_buckets_update_bucket"),
        );
        let result = self.inner.mgmt.update_bucket(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.update_bucket(opts).await
    }

    pub async fn delete_bucket(&self, opts: &DeleteBucketOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), None, None),
            create_span!("manager_buckets_drop_bucket"),
        );
        let result = self.inner.mgmt.delete_bucket(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.delete_bucket(opts).await
    }

    pub async fn flush_bucket(&self, opts: &FlushBucketOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            build_keyspace(Some(opts.bucket_name), None, None),
            create_span!("manager_buckets_flush_bucket"),
        );
        let result = self.inner.mgmt.flush_bucket(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.flush_bucket(opts).await
    }

    pub async fn get_user(&self, opts: &GetUserOptions<'_>) -> Result<UserAndMetadata> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_get_user"),
        );
        let result = self.inner.mgmt.get_user(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_user(opts).await
    }

    pub async fn get_all_users(
        &self,
        opts: &GetAllUsersOptions<'_>,
    ) -> Result<Vec<UserAndMetadata>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_get_all_users"),
        );
        let result = self.inner.mgmt.get_all_users(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_all_users(opts).await
    }

    pub async fn upsert_user(&self, opts: &UpsertUserOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_upsert_user"),
        );
        let result = self.inner.mgmt.upsert_user(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.upsert_user(opts).await
    }

    pub async fn delete_user(&self, opts: &DeleteUserOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_drop_user"),
        );
        let result = self.inner.mgmt.delete_user(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.delete_user(opts).await
    }

    pub async fn get_roles(&self, opts: &GetRolesOptions<'_>) -> Result<Vec<RoleAndDescription>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_get_roles"),
        );
        let result = self.inner.mgmt.get_roles(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_roles(opts).await
    }

    pub async fn get_group(&self, opts: &GetGroupOptions<'_>) -> Result<Group> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_get_group"),
        );
        let result = self.inner.mgmt.get_group(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_group(opts).await
    }

    pub async fn get_all_groups(&self, opts: &GetAllGroupsOptions<'_>) -> Result<Vec<Group>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_get_all_groups"),
        );
        let result = self.inner.mgmt.get_all_groups(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.get_all_groups(opts).await
    }

    pub async fn upsert_group(&self, opts: &UpsertGroupOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_upsert_group"),
        );
        let result = self.inner.mgmt.upsert_group(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.upsert_group(opts).await
    }

    pub async fn delete_group(&self, opts: &DeleteGroupOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_drop_group"),
        );
        let result = self.inner.mgmt.delete_group(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.delete_group(opts).await
    }

    pub async fn change_password(&self, opts: &ChangePasswordOptions<'_>) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("manager_users_change_password"),
        );
        let result = self.inner.mgmt.change_password(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.mgmt.change_password(opts).await
    }

    pub async fn ping(&self, opts: &PingOptions) -> Result<PingReport> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("ping"),
        );
        let result = self.inner.diagnostics.ping(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.diagnostics.ping(opts).await
    }

    pub async fn ensure_user(&self, opts: &EnsureUserOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_user(opts).await
    }

    pub async fn ensure_group(&self, opts: &EnsureGroupOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_group(opts).await
    }

    pub async fn ensure_bucket(&self, opts: &EnsureBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.ensure_bucket(opts).await
    }

    pub async fn ensure_search_index(
        &self,
        opts: &search_management::EnsureIndexOptions<'_>,
    ) -> Result<()> {
        self.inner.search.ensure_index(opts).await
    }

    pub async fn wait_until_ready(&self, opts: &WaitUntilReadyOptions) -> Result<()> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("wait_until_ready"),
        );
        let result = self.inner.diagnostics.wait_until_ready(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.diagnostics.wait_until_ready(opts).await
    }

    pub async fn diagnostics(&self, opts: &DiagnosticsOptions) -> Result<DiagnosticsResult> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_MANAGEMENT),
            Keyspace::Cluster,
            create_span!("diagnostics"),
        );
        let result = self.inner.diagnostics.diagnostics(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.diagnostics.diagnostics(opts).await
    }

    pub async fn analytics_query(&self, opts: AnalyticsOptions) -> Result<AnalyticsResultStream> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_ANALYTICS),
            Keyspace::Cluster,
            create_span!("analytics")
                        .with_statement(opts.statement.as_deref().unwrap_or("")),
        );
        let result = self.inner.analytics.query(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.analytics.query(opts).await
    }

    pub async fn analytics_get_pending_mutations(
        &self,
        opts: &GetPendingMutationsOptions<'_>,
    ) -> Result<HashMap<String, HashMap<String, i64>>> {
        if self.inner.tracing.core_observability_enabled() {
        let ctx = self.begin_operation(
            Some(crate::tracingcomponent::SERVICE_VALUE_ANALYTICS),
            Keyspace::Cluster,
            create_span!("manager_analytics_get_pending_mutations"),
        );
        let result = self.inner.analytics.get_pending_mutations(opts)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        return result;
        }
        self.inner.analytics.get_pending_mutations(opts).await
    }

    pub async fn get_full_bucket_config(
        &self,
        opts: &GetFullBucketConfigOptions<'_>,
    ) -> Result<FullBucketConfig> {
        self.inner.mgmt.get_full_bucket_config(opts).await
    }

    pub async fn get_full_cluster_config(
        &self,
        opts: &GetFullClusterConfigOptions<'_>,
    ) -> Result<FullClusterConfig> {
        self.inner.mgmt.get_full_cluster_config(opts).await
    }

    pub async fn load_sample_bucket(&self, opts: &LoadSampleBucketOptions<'_>) -> Result<()> {
        self.inner.mgmt.load_sample_bucket(opts).await
    }

    pub async fn index_status(&self, opts: &IndexStatusOptions<'_>) -> Result<IndexStatus> {
        self.inner.mgmt.index_status(opts).await
    }

    pub async fn get_auto_failover_settings(
        &self,
        opts: &GetAutoFailoverSettingsOptions<'_>,
    ) -> Result<AutoFailoverSettings> {
        self.inner.mgmt.get_auto_failover_settings(opts).await
    }

    pub async fn get_bucket_stats(
        &self,
        opts: &GetBucketStatsOptions<'_>,
    ) -> Result<Box<RawValue>> {
        self.inner.mgmt.get_bucket_stats(opts).await
    }
}
