use crate::collection::Collection;
use crate::error::CouchbaseError;
use crate::instance::Instance;
use crate::options::{AnalyticsOptions, QueryOptions};
use crate::result::{AnalyticsResult, QueryResult};
use futures::Future;
use std::sync::Arc;

/// Provides access to `Bucket` level operations and `Collections`.
pub struct Bucket {
    instance: Arc<Instance>,
}

impl Bucket {
    /// Internal method to create a new bucket, which in turn creates the lcb instance
    /// attached to this bucket.
    pub(crate) fn new(cs: &str, user: &str, pw: &str) -> Result<Self, CouchbaseError> {
        let instance = Instance::new(cs, user, pw)?;
        Ok(Bucket {
            instance: Arc::new(instance),
        })
    }

    /// Opens the default `Collection`.
    ///
    /// This method provides access to the default collection, which is present if you do
    /// not have any collections (upgrading from an older cluster) or if you are on a
    /// Couchbase Server version which does not support collections yet.
    pub fn default_collection(&self) -> Collection {
        Collection::new(self.instance.clone())
    }

    /// Internal proxy method that gets called from the cluster so we can send it into the
    /// instance.
    pub(crate) fn query<S>(
        &self,
        statement: S,
        options: Option<QueryOptions>,
    ) -> impl Future<Item = QueryResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.query(statement.into(), options)
    }

    /// Internal proxy method that gets called from the cluster so we can send it into the
    /// instance.
    pub(crate) fn analytics_query<S>(
        &self,
        statement: S,
        options: Option<AnalyticsOptions>,
    ) -> impl Future<Item = AnalyticsResult, Error = CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.analytics_query(statement.into(), options)
    }

    /// Internal proxy method that gets called from the cluster so we can send it into the
    /// instance.
    pub(crate) fn close(&self) -> Result<(), CouchbaseError> {
        self.instance.shutdown()
    }
}
