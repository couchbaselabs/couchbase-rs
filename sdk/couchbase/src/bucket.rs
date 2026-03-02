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

//! A Couchbase [`Bucket`] and its associated operations.
//!
//! A `Bucket` is obtained from [`Cluster::bucket`](crate::cluster::Cluster::bucket) and provides
//! access to [`Scope`]s and [`Collection`]s,
//! as well as bucket-level diagnostics and collection management.

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
use crate::tracing::Keyspace;
use couchbase_core::create_span;
use tracing::Instrument;

/// Represents a Couchbase bucket.
///
/// A `Bucket` provides access to [`Scope`]s and
/// [`Collection`]s, as well as bucket-level
/// diagnostics and collection management.
///
/// Obtain a `Bucket` by calling [`Cluster::bucket`](crate::cluster::Cluster::bucket).
///
/// `Bucket` is cheaply cloneable.
///
/// # Example
///
/// ```rust,no_run
/// # use couchbase::cluster::Cluster;
/// # use couchbase::authenticator::PasswordAuthenticator;
/// # use couchbase::options::cluster_options::ClusterOptions;
/// # async fn example() -> couchbase::error::Result<()> {
/// # let cluster = Cluster::connect("couchbase://localhost",
/// #     ClusterOptions::new(PasswordAuthenticator::new("u", "p").into())).await?;
/// let bucket = cluster.bucket("travel-sample");
///
/// // Access the default collection
/// let collection = bucket.default_collection();
///
/// // Access a named scope and collection
/// let scope = bucket.scope("inventory");
/// let collection = scope.collection("airline");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Bucket {
    client: BucketClient,
    collections_mgmt_client: CollectionsMgmtClient,
    diagnostics_client: DiagnosticsClient,
    tracing_client: TracingClient,
}

impl Bucket {
    pub(crate) fn new(client: BucketClient) -> Self {
        let collections_mgmt_client = client.collections_management_client();
        let diagnostics_client = client.diagnostics_client();
        let tracing_client = client.tracing_client();

        Self {
            client,
            collections_mgmt_client,
            diagnostics_client,
            tracing_client,
        }
    }

    fn keyspace(&self) -> Keyspace<'_> {
        Keyspace::Bucket {
            bucket: self.client.name(),
        }
    }

    /// Returns the name of this bucket.
    pub fn name(&self) -> &str {
        self.client.name()
    }

    /// Returns a [`Scope`] with the given name.
    ///
    /// Use `"_default"` for the default scope.
    pub fn scope(&self, name: impl Into<String>) -> Scope {
        Scope::new(self.client.scope_client(name.into()))
    }

    /// Returns a [`Collection`] with the given name
    /// from the default scope (`_default`).
    ///
    /// Equivalent to `bucket.scope("_default").collection(name)`.
    pub fn collection(&self, name: impl Into<String>) -> Collection {
        self.scope("_default").collection(name)
    }

    /// Returns the default collection (`_default`) from the default scope.
    ///
    /// Equivalent to `bucket.collection("_default")`.
    pub fn default_collection(&self) -> Collection {
        self.collection("_default".to_string())
    }

    /// Returns a [`CollectionManager`]
    /// for creating, dropping, and listing scopes and collections in this bucket.
    pub fn collections(&self) -> CollectionManager {
        CollectionManager {
            client: self.collections_mgmt_client.clone(),
        }
    }

    /// Pings the endpoints associated with this bucket and returns a
    /// [`PingReport`] with latency data.
    pub async fn ping(&self, opts: impl Into<Option<PingOptions>>) -> error::Result<PingReport> {
        let keyspace = self.keyspace();
        let ctx = self
            .tracing_client
            .begin_operation(None, keyspace, create_span!("ping"))
            .await;
        let result = self
            .diagnostics_client
            .ping(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Waits until the bucket is ready (all configured service types are connected).
    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        let keyspace = self.keyspace();
        let ctx = self
            .tracing_client
            .begin_operation(None, keyspace, create_span!("wait_until_ready"))
            .await;
        let result = self
            .diagnostics_client
            .wait_until_ready(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
