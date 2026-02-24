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
    Keyspace, SpanBuilder, SERVICE_VALUE_QUERY, SERVICE_VALUE_SEARCH, SPAN_ATTRIB_DB_SYSTEM_VALUE,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use couchbase_core::create_span;
use std::sync::Arc;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct Scope {
    client: ScopeClient,
    query_client: Arc<QueryClient>,
    search_client: Arc<SearchClient>,
    search_index_client: Arc<SearchIndexMgmtClient>,
    tracing_client: TracingClient,
    keyspace: Keyspace,
}

impl Scope {
    pub(crate) fn new(client: ScopeClient) -> Self {
        let query_client = Arc::new(client.query_client());
        let search_client = Arc::new(client.search_client());
        let search_index_client = Arc::new(client.search_index_management_client());
        let tracing_client = client.tracing_client();
        let keyspace = Keyspace::Scope {
            bucket: client.bucket_name().to_string(),
            scope: client.name().to_string(),
        };

        Self {
            client,
            query_client,
            search_client,
            search_index_client,
            tracing_client,
            keyspace,
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

    pub async fn search(
        &self,
        index_name: impl Into<String>,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.search_internal(index_name.into(), request, opts).await
    }

    async fn query_internal(
        &self,
        statement: String,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        let span = create_span!("query").with_statement(&statement);

        self.tracing_client
            .execute_observable_operation(Some(SERVICE_VALUE_QUERY), &self.keyspace, span, || {
                self.query_client.query(statement, opts.into())
            })
            .await
    }

    async fn search_internal(
        &self,
        index_name: String,
        request: SearchRequest,
        opts: impl Into<Option<SearchOptions>>,
    ) -> error::Result<SearchResult> {
        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_SEARCH),
                &self.keyspace,
                create_span!("search"),
                || self.search_client.search(index_name, request, opts.into()),
            )
            .await
    }

    pub fn search_indexes(&self) -> SearchIndexManager {
        SearchIndexManager {
            client: self.search_index_client.clone(),
        }
    }
}
