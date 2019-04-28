use crate::bucket::Bucket;

use crate::error::CouchbaseError;
use crate::options::{AnalyticsOptions, QueryOptions};
use crate::result::{AnalyticsResult, QueryResult};
use std::collections::HashMap;
use std::sync::Arc;

pub struct Cluster {
    connection_string: String,
    username: String,
    password: String,
    buckets: HashMap<String, Arc<Bucket>>,
}

impl Cluster {
    pub fn connect<S>(connection_string: S, username: S, password: S) -> Self
    where
        S: Into<String>,
    {
        Cluster {
            connection_string: connection_string.into(),
            username: username.into(),
            password: password.into(),
            buckets: HashMap::new(),
        }
    }

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

    pub fn query<S>(
        &self,
        statement: S,
        options: Option<QueryOptions>,
    ) -> Result<QueryResult, CouchbaseError>
    where
        S: Into<String>,
    {
        let bucket = match self.buckets.values().nth(0) {
            Some(b) => b,
            None => panic!("At least one bucket needs to be open to perform a query for now!"),
        };

        bucket.query(statement, options)
    }

    pub fn analytics_query<S>(
        &self,
        statement: S,
        options: Option<AnalyticsOptions>,
    ) -> Result<AnalyticsResult, CouchbaseError>
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

    pub fn disconnect(&mut self) {
        for bucket in self.buckets.values() {
            bucket.close();
        }
    }
}
