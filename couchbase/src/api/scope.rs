use super::*;
use crate::io::request::{AnalyticsRequest, QueryRequest, Request};
use crate::{
    AnalyticsOptions, AnalyticsResult, Collection, CouchbaseResult, QueryOptions, QueryResult,
};
use futures::channel::oneshot;
use std::sync::Arc;

/// Scopes provide access to a group of collections
#[derive(Debug)]
pub struct Scope {
    bucket_name: String,
    name: String,
    core: Arc<Core>,
}

impl Scope {
    pub(crate) fn new(core: Arc<Core>, name: String, bucket_name: String) -> Self {
        Self {
            core,
            name,
            bucket_name,
        }
    }

    /// The name of the scope
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Opens a custom collection inside the current scope
    ///
    /// # Arguments
    ///
    /// * `name` - the collection name
    pub fn collection(&self, name: impl Into<String>) -> Collection {
        Collection::new(
            self.core.clone(),
            name.into(),
            self.name.clone(),
            self.bucket_name.clone(),
        )
    }

    /// Executes a N1QL statement
    ///
    /// # Arguments
    ///
    /// * `statement` - the N1QL statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a N1QL query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.query("select * from bucket", couchbase::QueryOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// let bucket = cluster.bucket("default");
    /// let scope = bucket.scope("myscope");
    /// match scope.query("select 1=1", couchbase::QueryOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [QueryResult](struct.QueryResult.html) for more information on what and how it can be consumed.
    pub async fn query(
        &self,
        statement: impl Into<String>,
        options: impl Into<Option<QueryOptions>>,
    ) -> CouchbaseResult<QueryResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options,
            sender,
            scope: Some(self.name.clone()),
        }));
        receiver.await.unwrap()
    }

    /// Executes an analytics query
    ///
    /// # Arguments
    ///
    /// * `statement` - the analyticss statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run an analytics query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.analytics_query("select * from dataset", couchbase::AnalyticsOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// match cluster.analytics_query("select 1=1", couchbase::AnalyticsOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [AnalyticsResult](struct.AnalyticsResult.html) for more information on what and how it can be consumed.
    pub async fn analytics_query(
        &self,
        statement: impl Into<String>,
        options: impl Into<Option<AnalyticsOptions>>,
    ) -> CouchbaseResult<AnalyticsResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options,
            sender,
            scope: Some(self.name.clone()),
        }));
        receiver.await.unwrap()
    }
}
