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

use crate::clients::query_client::QueryClient;
use crate::clients::scope_client::ScopeClient;
use crate::clients::search_client::SearchClient;
use crate::clients::search_index_mgmt_client::SearchIndexMgmtClient;
use crate::clients::tracing_client::TracingClient;
use crate::collection::Collection;
use crate::error;
use crate::management::search::search_index_manager::SearchIndexManager;
use crate::options::query_options::QueryOptions;
use crate::options::search_options::SearchOptions;
use crate::results::query_results::QueryResult;
use crate::results::search_results::SearchResult;
use crate::search::request::SearchRequest;
use crate::tracing::{
    SERVICE_VALUE_QUERY, SERVICE_VALUE_SEARCH, SPAN_ATTRIB_DB_SYSTEM_VALUE,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    search_index_client: Arc<SearchIndexMgmtClient>,
    tracing_client: TracingClient,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let search_index_client = Arc::new(client.search_index_management_client());
        let tracing_client = client.tracing_client();

        Self {
            client,
            query_client,
            search_client,
            search_index_client,
            tracing_client,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn collection(&self, name: impl Into<String>) -> Collection {
        Collection::new(self.client.collection_client(name.into()))
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.query_internal(statement.into(), opts).await
    }

    #[instrument(
    skip_all,
    level = Level::TRACE,
    name = "query",
    fields(
    otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
    db.operation.name = "query",
    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
    db.query.text = statement,
    db.namespace = self.client.bucket_name(),
    couchbase.scope.name = self.name(),
    couchbase.service = SERVICE_VALUE_QUERY,
    couchbase.retries = 0,
    couchbase.cluster.name,
    couchbase.cluster.uuid,
    ))]
    async fn query_internal(
        &self,
        statement: String,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.tracing_client.record_generic_fields().await;
        self.query_client.query(statement, opts.into()).await
    }

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_internal(index_name.into(), request, opts).await
    }

    #[instrument(
    skip_all,
    level = Level::TRACE,
    name = "search",
    fields(
    otel.kind = SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
    db.operation.name = "search",
    db.system.name = SPAN_ATTRIB_DB_SYSTEM_VALUE,
    db.namespace = self.client.bucket_name(),
    couchbase.scope.name = self.name(),
    couchbase.service = SERVICE_VALUE_SEARCH,
    couchbase.retries = 0,
    couchbase.cluster.name,
    couchbase.cluster.uuid,
    ))]
    async fn search_internal(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.tracing_client.record_generic_fields().await;
        self.search_client
            .search(index_name, request, opts.into())
            .await
    }

    pub fn search_indexes(&self) -> SearchIndexManager {
        SearchIndexManager {
            client: self.search_index_client.clone(),
        }
    }
}
