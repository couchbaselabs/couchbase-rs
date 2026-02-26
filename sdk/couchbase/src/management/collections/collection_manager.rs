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
use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::error;
pub use crate::management::collections::collection_settings::{
    CreateCollectionSettings, UpdateCollectionSettings,
};
use crate::options::collection_mgmt_options::*;
use crate::results::collections_mgmt_results::ScopeSpec;
use crate::tracing::{Keyspace, SERVICE_VALUE_MANAGEMENT};
use couchbase_core::create_span;
use tracing::Instrument;

#[derive(Clone)]
pub struct CollectionManager {
    pub(crate) client: CollectionsMgmtClient,
}

impl CollectionManager {
    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        let scope_name: String = scope_name.into();
        let keyspace = Keyspace::Scope {
            bucket: self.client.bucket_name(),
            scope: &scope_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_create_scope"),
            )
            .await;
        let result = self
            .client
            .create_scope(scope_name.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        let scope_name: String = scope_name.into();
        let keyspace = Keyspace::Scope {
            bucket: self.client.bucket_name(),
            scope: &scope_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_drop_scope"),
            )
            .await;
        let result = self
            .client
            .drop_scope(scope_name.clone(), opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: impl Into<Option<CreateCollectionSettings>>,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        let scope_name: String = scope_name.into();
        let collection_name: String = collection_name.into();
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name(),
            scope: &scope_name,
            collection: &collection_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_create_collection"),
            )
            .await;
        let result = self
            .client
            .create_collection(
                scope_name.clone(),
                collection_name.clone(),
                settings.into().unwrap_or_default(),
                opts.into().unwrap_or_default(),
            )
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        let scope_name: String = scope_name.into();
        let collection_name: String = collection_name.into();
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name(),
            scope: &scope_name,
            collection: &collection_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_update_collection"),
            )
            .await;
        let result = self
            .client
            .update_collection(
                scope_name.clone(),
                collection_name.clone(),
                settings,
                opts.into().unwrap_or_default(),
            )
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        let scope_name: String = scope_name.into();
        let collection_name: String = collection_name.into();
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name(),
            scope: &scope_name,
            collection: &collection_name,
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_drop_collection"),
            )
            .await;
        let result = self
            .client
            .drop_collection(
                scope_name.clone(),
                collection_name.clone(),
                opts.into().unwrap_or_default(),
            )
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    pub async fn get_all_scopes(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        let keyspace = Keyspace::Bucket {
            bucket: self.client.bucket_name(),
        };
        let ctx = self
            .client
            .tracing_client()
            .begin_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                keyspace,
                create_span!("manager_collections_get_all_scopes"),
            )
            .await;
        let result = self
            .client
            .get_all_scopes(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
