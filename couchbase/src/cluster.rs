use crate::bucket::Bucket;

use crate::error::CouchbaseError;
use crate::options::{AnalyticsOptions, QueryOptions};
use crate::result::{AnalyticsResult, QueryResult};
use futures::Future;
use std::collections::HashMap;
use std::sync::Arc;

/// The `Cluster` is the main entry point when working with the client.
pub struct Cluster {
    connection_string: String,
    username: String,
    password: String,
    buckets: HashMap<String, Arc<Bucket>>,
}

impl Cluster {
    /// Creates a new connection reference to the Couchbase cluster.
    ///
    /// Keep in mind that only Role-Based access control (RBAC) is supported, so you need to configure
    /// a username and password on the cluster. This implies that only Couchbase Server versions
    /// 5.0 and later are supported.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - Holds the bootstrap hostnames and optionally config settings.
    /// * `username` - The name of the user configured on the cluster.
    /// * `password` - The password of the user configured on the cluster.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use couchbase::Cluster;
    ///
    /// let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    ///   .expect("Could not create cluster reference");
    /// ```
    ///
    pub fn connect<S>(
        connection_string: S,
        username: S,
        password: S,
    ) -> Result<Self, CouchbaseError>
    where
        S: Into<String>,
    {
        Ok(Cluster {
            connection_string: connection_string.into(),
            username: username.into(),
            password: password.into(),
            buckets: HashMap::new(),
        })
    }

    /// Opens a Couchbase bucket.
    ///
    /// If you wonder why this returns an `Arc`, the reason is that we also need to keep track
    /// of the `Bucket` internally so if you call `disconnect` on the `Cluster` all opened
    /// buckets are closed. Also, we make sure that if this method is called more than once on
    /// the same bucket, it is only opened once since buckets are expensive resources with state
    /// attached (for those familiar with libcouchbase: the bucket internally holds the lcb
    /// instance).
    ///
    /// We recommend only ever opening a bucket once and then reusing it across the lifetime of
    /// your application for maximum performance and resource efficiency.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// #
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #     .expect("Could not create cluster reference");
    /// let bucket = cluster.bucket("travel-sample")
    ///     .expect("Could not open bucket");
    /// ```
    ///
    pub fn bucket<S>(&mut self, name: S) -> Result<Arc<Bucket>, CouchbaseError>
    where
        S: Into<String>,
    {
        let name = name.into();
        let bucket = Arc::new(Bucket::new(
            &format!("{}/{}", self.connection_string, name.clone()),
            &self.username,
            &self.password,
        )?);

        self.buckets.insert(name.clone(), bucket.clone());
        Ok(bucket.clone())
    }

    /// Performs a query against the N1QL query service.
    ///
    /// For now, please make sure to open one bucket (doesn't matter which one) before performing
    /// a cluster-level query. This limiation will be lifted in the future, but for now the client
    /// needs an open bucket so it knows where internally to route the query.
    ///
    /// # Arguments
    ///
    /// * `statement` - The query string itself.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use futures::Future;
    /// use serde_json::Value;
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #    .expect("Could not create cluster reference");
    /// # let _ = cluster.bucket("travel-sample");
    /// #
    /// let mut result = cluster.query("select name, type from `travel-sample` limit 5", None)
    ///     .wait()
    ///     .expect("Could not perform query");
    ///
    /// println!("Rows:\n{:?}", result.rows_as().collect::<Vec<Value>>());
    /// println!("Meta:\n{:?}", result.meta());
    /// ```
    ///
    pub fn query<S>(
        &self,
        statement: S,
        options: Option<QueryOptions>,
    ) -> impl Future<Item = QueryResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        let bucket = match self.buckets.values().nth(0) {
            Some(b) => b,
            None => panic!("At least one bucket needs to be open to perform a query for now!"),
        };

        bucket.query(statement, options)
    }

    /// Performs a query against the analytics service.
    ///
    /// For now, please make sure to open one bucket (doesn't matter which one) before performing
    /// a cluster-level analytics query. This limiation will be lifted in the future, but for now
    /// the client needs an open bucket so it knows where internally to route the query.
    ///
    /// # Arguments
    ///
    /// * `statement` - The analytics query string itself.
    /// * `options` - Options to customize the default behavior.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// use futures::Future;
    /// use serde_json::Value;
    /// #
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #     .expect("Could not create cluster reference!");
    /// # let _ = cluster.bucket("travel-sample");
    /// #
    /// let mut result = cluster
    ///     .analytics_query("SELECT DataverseName FROM Metadata.`Dataverse`", None)
    ///     .wait()
    ///     .expect("Could not perform analytics query");
    ///
    /// println!("---> rows {:?}", result.rows_as().collect::<Vec<Value>>());
    /// println!("---> meta {:?}", result.meta());
    /// ```
    ///
    pub fn analytics_query<S>(
        &self,
        statement: S,
        options: Option<AnalyticsOptions>,
    ) -> impl Future<Item = AnalyticsResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        let bucket = match self.buckets.values().nth(0) {
            Some(b) => b,
            None => panic!(
                "At least one bucket needs to be open to perform an analytics query for now!"
            ),
        };

        bucket.analytics_query(statement, options)
    }

    /// Disconnects this cluster and all associated open buckets.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use couchbase::Cluster;
    /// #
    /// # let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
    /// #    .expect("Could not create cluster reference!");
    /// #
    /// cluster.disconnect().expect("Could not shutdown properly");
    /// ```
    ///
    pub fn disconnect(&mut self) -> Result<(), CouchbaseError> {
        for bucket in self.buckets.values() {
            bucket.close()?;
        }
        self.buckets.clear();
        Ok(())
    }
}
