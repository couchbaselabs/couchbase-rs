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

use crate::authenticator::Authenticator;
use crate::bucket::Bucket;
use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::diagnostics_client::DiagnosticsClient;
use crate::clients::query_client::QueryClient;
use crate::clients::tracing_client::TracingClient;
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::buckets::bucket_manager::BucketManager;
use crate::management::users::user_manager::UserManager;
use crate::options::cluster_options::ClusterOptions;
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::options::query_options::QueryOptions;
use crate::results::diagnostics::{DiagnosticsResult, PingReport};
use crate::results::query_results::QueryResult;
use crate::tracing::{
    Keyspace, SpanBuilder, SERVICE_VALUE_QUERY, SPAN_ATTRIB_DB_SYSTEM_VALUE,
    SPAN_ATTRIB_OTEL_KIND_CLIENT_VALUE,
};
use log::info;
use std::sync::Arc;
use tracing::instrument;
use tracing::Level;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    query_client: Arc<QueryClient>,
    bucket_mgmt_client: Arc<BucketMgmtClient>,
    user_mgmt_client: Arc<UserMgmtClient>,
    diagnostics_client: Arc<DiagnosticsClient>,
    tracing_client: Arc<TracingClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        info!("SDK Version: {}", env!("CARGO_PKG_VERSION"));
        info!("Cluster Options {opts}");
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let query_client = Arc::new(client.query_client());
        let bucket_mgmt_client = Arc::new(client.buckets_client());
        let user_mgmt_client = Arc::new(client.users_client());
        let diagnostics_client = Arc::new(client.diagnostics_client());
        let tracing_client = Arc::new(client.tracing_client());

        Ok(Cluster {
            client,
            query_client,
            bucket_mgmt_client,
            user_mgmt_client,
            diagnostics_client,
            tracing_client,
        })
    }

    pub fn bucket(&self, name: impl Into<String>) -> Bucket {
        let bucket_client = self.client.bucket_client(name.into());

        Bucket::new(bucket_client)
    }

    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        self.query_client.query(statement.into(), opts.into()).await
    }

    pub fn buckets(&self) -> BucketManager {
        BucketManager::new(self.bucket_mgmt_client.clone())
    }

    pub fn users(&self) -> UserManager {
        UserManager::new(self.user_mgmt_client.clone())
    }

    pub async fn diagnostics(
        &self,
        opts: impl Into<Option<DiagnosticsOptions>>,
    ) -> error::Result<DiagnosticsResult> {
        self.diagnostics_internal(opts).await
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

    // Sets a new authenticator for the cluster.
    // For KV the new Authenticator does not take effect until connections are re-established.
    // For HTTP the behaviour depends on the Authenticator type.
    // Authenticators which apply authentication per-request (such as PasswordAuthenticator) will take effect immediately
    // but transport level Authenticators (such as CertificateAuthenticator) will not take effect until new connections
    // are created.
    pub async fn set_authenticator(&self, authenticator: Authenticator) -> error::Result<()> {
        self.client.set_authenticator(authenticator).await
    }

    async fn query_internal(
        &self,
        statement: String,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        let span = create_span!("query").with_statement(&statement);

        self.tracing_client
            .execute_observable_operation(
                Some(SERVICE_VALUE_QUERY),
                &Keyspace::Cluster,
                span,
                self.query_client.query(statement, opts.into()),
            )
            .await
    }

    async fn ping_internal(
        &self,
        opts: impl Into<Option<PingOptions>>,
    ) -> error::Result<PingReport> {
        self.tracing_client
            .execute_observable_operation(
                None,
                &Keyspace::Cluster,
                create_span!("ping"),
                self.diagnostics_client
                    .ping(opts.into().unwrap_or_default()),
            )
            .await
    }

    async fn diagnostics_internal(
        &self,
        opts: impl Into<Option<DiagnosticsOptions>>,
    ) -> error::Result<DiagnosticsResult> {
        self.tracing_client
            .execute_observable_operation(
                None,
                &Keyspace::Cluster,
                create_span!("diagnostics"),
                self.diagnostics_client
                    .diagnostics(opts.into().unwrap_or_default()),
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
                &Keyspace::Cluster,
                create_span!("wait_until_ready"),
                self.diagnostics_client
                    .wait_until_ready(opts.into().unwrap_or_default()),
            )
            .await
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        info!("Dropping Cluster");
    }
}
