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

use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::options::query_options::QueryOptions;
use crate::results::query_results::QueryResult;
use uuid::Uuid;

pub(crate) struct QueryClient {
    backend: QueryClientBackend,
}

impl QueryClient {
    pub fn new(backend: QueryClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        match &self.backend {
            QueryClientBackend::CouchbaseQueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
            QueryClientBackend::Couchbase2QueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
        }
    }
}

pub(crate) enum QueryClientBackend {
    CouchbaseQueryClientBackend(CouchbaseQueryClient),
    Couchbase2QueryClientBackend(Couchbase2QueryClient),
}

pub(crate) struct QueryKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

pub(crate) struct CouchbaseQueryClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: Option<QueryKeyspace>,
}

impl CouchbaseQueryClient {
    pub fn new(agent_provider: CouchbaseAgentProvider) -> Self {
        Self {
            agent_provider,
            keyspace: None,
        }
    }

    pub fn with_keyspace(agent_provider: CouchbaseAgentProvider, keyspace: QueryKeyspace) -> Self {
        Self {
            agent_provider,
            keyspace: Some(keyspace),
        }
    }

    async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        let mut opts = opts.unwrap_or_default();
        if opts.client_context_id.is_none() {
            opts.client_context_id = Some(Uuid::new_v4().to_string());
        }

        let ad_hoc = opts.ad_hoc.unwrap_or(true);

        let mut query_opts = couchbase_core::options::query::QueryOptions::try_from(opts)?;
        query_opts = query_opts.statement(statement);

        if let Some(keyspace) = &self.keyspace {
            query_opts = query_opts.query_context(format!(
                "`{}`.`{}`",
                keyspace.bucket_name.clone(),
                keyspace.scope_name.clone()
            ));
        }

        let agent = self.agent_provider.get_agent().await;
        if ad_hoc {
            Ok(QueryResult::from(
                CouchbaseAgentProvider::upgrade_agent(agent)?
                    .query(query_opts)
                    .await?,
            ))
        } else {
            Ok(QueryResult::from(
                CouchbaseAgentProvider::upgrade_agent(agent)?
                    .prepared_query(query_opts)
                    .await?,
            ))
        }
    }
}

pub(crate) struct Couchbase2QueryClient {}

impl Couchbase2QueryClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn query(
        &self,
        _statement: String,
        _opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        unimplemented!()
    }
}
