use crate::collection::Collection;
use crate::instance::Instance;
use crate::options::{AnalyticsOptions, QueryOptions};
use crate::result::{AnalyticsResult, QueryResult};
use futures::Future;
use std::sync::Arc;
use crate::error::CouchbaseError;

pub struct Bucket {
    instance: Arc<Instance>,
}

impl Bucket {
    pub fn new(cs: &str, user: &str, pw: &str) -> Result<Self, CouchbaseError> {
        let instance = Instance::new(cs, user, pw)?;
        Ok(Bucket {
            instance: Arc::new(instance),
        })
    }

    pub fn default_collection(&self) -> Collection {
        Collection::new(self.instance.clone())
    }

    pub(crate) fn query<S>(
        &self,
        statement: S,
        options: Option<QueryOptions>,
    ) -> Result<QueryResult, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance.query(statement.into(), options).wait()
    }

    pub(crate) fn analytics_query<S>(
        &self,
        statement: S,
        options: Option<AnalyticsOptions>,
    ) -> Result<AnalyticsResult, CouchbaseError>
    where
        S: Into<String>,
    {
        self.instance
            .analytics_query(statement.into(), options)
            .wait()
    }

    pub(crate) fn close(&self) {
        self.instance.shutdown();
    }
}
