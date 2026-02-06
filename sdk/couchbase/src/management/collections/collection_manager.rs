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
use crate::tracing::{
    Keyspace, SpanBuilder, SERVICE_VALUE_MANAGEMENT, SPAN_ATTRIB_DB_SYSTEM_VALUE,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};

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
        self.create_scope_internal(scope_name.into(), opts).await
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        self.drop_scope_internal(scope_name.into(), opts).await
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: impl Into<Option<CreateCollectionSettings>>,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        self.create_collection_internal(scope_name.into(), collection_name.into(), settings, opts)
            .await
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        self.update_collection_internal(scope_name.into(), collection_name.into(), settings, opts)
            .await
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        self.drop_collection_internal(scope_name.into(), collection_name.into(), opts.into())
            .await
    }

    pub async fn get_all_scopes(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        self.get_all_scopes_internal(opts).await
    }

    async fn create_scope_internal(
        &self,
        scope_name: String,
        opts: impl Into<Option<CreateScopeOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Scope {
            bucket: self.client.bucket_name().to_string(),
            scope: scope_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_create_scope"),
                self.client
                    .create_scope(scope_name, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn drop_scope_internal(
        &self,
        scope_name: String,
        opts: impl Into<Option<DropScopeOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Scope {
            bucket: self.client.bucket_name().to_string(),
            scope: scope_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_drop_scope"),
                self.client
                    .drop_scope(scope_name, opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn create_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        settings: impl Into<Option<CreateCollectionSettings>>,
        opts: impl Into<Option<CreateCollectionOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name().to_string(),
            scope: scope_name.clone(),
            collection: collection_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_create_collection"),
                self.client.create_collection(
                    scope_name,
                    collection_name,
                    settings.into().unwrap_or_default(),
                    opts.into().unwrap_or_default(),
                ),
            )
            .await
    }

    async fn update_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        settings: impl Into<UpdateCollectionSettings>,
        opts: impl Into<Option<UpdateCollectionOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name().to_string(),
            scope: scope_name.clone(),
            collection: collection_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_update_collection"),
                self.client.update_collection(
                    scope_name,
                    collection_name,
                    settings.into(),
                    opts.into().unwrap_or_default(),
                ),
            )
            .await
    }

    async fn drop_collection_internal(
        &self,
        scope_name: String,
        collection_name: String,
        opts: impl Into<Option<DropCollectionOptions>>,
    ) -> error::Result<()> {
        let keyspace = Keyspace::Collection {
            bucket: self.client.bucket_name().to_string(),
            scope: scope_name.clone(),
            collection: collection_name.clone(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_drop_collection"),
                self.client.drop_collection(
                    scope_name,
                    collection_name,
                    opts.into().unwrap_or_default(),
                ),
            )
            .await
    }

    async fn get_all_scopes_internal(
        &self,
        opts: impl Into<Option<GetAllScopesOptions>>,
    ) -> error::Result<Vec<ScopeSpec>> {
        let keyspace = Keyspace::Bucket {
            bucket: self.client.bucket_name().to_string(),
        };

        self.client
            .tracing_client()
            .execute_observable_operation(
                Some(SERVICE_VALUE_MANAGEMENT),
                &keyspace,
                create_span!("manager_collections_get_all_scopes"),
                self.client.get_all_scopes(opts.into().unwrap_or_default()),
            )
            .await
    }
}
