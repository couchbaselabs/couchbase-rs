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
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::buckets::bucket_manager::BucketManager;
use crate::management::users::user_manager::UserManager;
use crate::options::cluster_options::ClusterOptions;
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::options::query_options::QueryOptions;
use crate::results::diagnostics::{DiagnosticsResult, PingReport};
use crate::results::query_results::QueryResult;
use log::info;
use std::sync::Arc;

#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    query_client: Arc<QueryClient>,
    bucket_mgmt_client: Arc<BucketMgmtClient>,
    user_mgmt_client: Arc<UserMgmtClient>,
    diagnostics_client: Arc<DiagnosticsClient>,
}

impl Cluster {
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let query_client = Arc::new(client.query_client());
        let bucket_mgmt_client = Arc::new(client.buckets_client());
        let user_mgmt_client = Arc::new(client.users_client());
        let diagnostics_client = Arc::new(client.diagnostics_client());

        Ok(Cluster {
            client,
            query_client,
            bucket_mgmt_client,
            user_mgmt_client,
            diagnostics_client,
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
        let opts = opts.into().unwrap_or_default();
        self.diagnostics_client.diagnostics(opts).await
    }

    pub async fn ping(&self, opts: impl Into<Option<PingOptions>>) -> error::Result<PingReport> {
        let opts = opts.into().unwrap_or_default();
        self.diagnostics_client.ping(opts).await
    }

    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        let opts = opts.into().unwrap_or_default();
        self.diagnostics_client.wait_until_ready(opts).await
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
}

impl Drop for Cluster {
    fn drop(&mut self) {
        info!("Dropping Cluster");
    }
}
