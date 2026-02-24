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
use crate::clients::bucket_client::BucketClient;
use crate::clients::collections_mgmt_client::CollectionsMgmtClient;
use crate::clients::diagnostics_client::DiagnosticsClient;
use crate::clients::tracing_client::TracingClient;
use crate::collection::Collection;
use crate::error;
use crate::management::collections::collection_manager::CollectionManager;
use crate::options::diagnostic_options::{PingOptions, WaitUntilReadyOptions};
use crate::results::diagnostics::PingReport;
use crate::scope::Scope;
use crate::tracing::{
    Keyspace, SpanBuilder, SPAN_ATTRIB_DB_SYSTEM_VALUE, SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use couchbase_core::create_span;
use tracing::{instrument, Level};

#[derive(Clone)]
pub struct Bucket {
    client: BucketClient,
    collections_mgmt_client: CollectionsMgmtClient,
    diagnostics_client: DiagnosticsClient,
    tracing_client: TracingClient,
    keyspace: Keyspace,
}

impl Bucket {
    pub(crate) fn new(client: BucketClient) -> Self {
        let collections_mgmt_client = client.collections_management_client();
        let diagnostics_client = client.diagnostics_client();
        let tracing_client = client.tracing_client();
        let keyspace = Keyspace::Bucket {
            bucket: client.name().to_string(),
        };

        Self {
            client,
            collections_mgmt_client,
            diagnostics_client,
            tracing_client,
            keyspace,
        }
    }

    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub fn scope(&self, name: impl Into<String>) -> Scope {
        Scope::new(self.client.scope_client(name.into()))
    }

    pub fn collection(&self, name: impl Into<String>) -> Collection {
        self.scope("_default").collection(name)
    }

    pub fn default_collection(&self) -> Collection {
        self.collection("_default".to_string())
    }

    pub fn collections(&self) -> CollectionManager {
        CollectionManager {
            client: self.collections_mgmt_client.clone(),
        }
    }

    pub async fn ping(&self, opts: impl Into<Option<PingOptions>>) -> error::Result<PingReport> {
        self.ping_internal(opts).await
    }

    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        self.wait_until_ready_internal(opts).await
    }

    async fn ping_internal(
        &self,
        opts: impl Into<Option<PingOptions>>,
    ) -> error::Result<PingReport> {
        self.tracing_client
            .execute_observable_operation(
                None,
                &self.keyspace,
                create_span!("ping"),
                || self.diagnostics_client
                    .ping(opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn wait_until_ready_internal(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        self.tracing_client
            .execute_observable_operation(
                None,
                &self.keyspace,
                create_span!("wait_until_ready"),
                || self.diagnostics_client
                    .wait_until_ready(opts.into().unwrap_or_default()),
            )
            .await
    }
}
