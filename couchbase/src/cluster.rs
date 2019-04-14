use crate::bucket::Bucket;

use crate::options::QueryOptions;
use crate::result::QueryResult;
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

    pub fn bucket<S>(&mut self, name: S) -> Arc<Bucket>
    where
        S: Into<String>,
    {
        let name = name.into();
        let bucket = Arc::new(Bucket::new(
            &format!("{}/{}", self.connection_string, name.clone()),
            &self.username,
            &self.password,
        ));

        self.buckets.insert(name.clone(), bucket.clone());
        bucket.clone()
    }

    pub fn query<S>(&self, statement: S, options: Option<QueryOptions>) -> Result<QueryResult, ()>
    where
        S: Into<String>,
    {
        let bucket = match self.buckets.values().nth(0) {
            Some(b) => b,
            None => panic!("At least one bucket needs to be open to perform a query for now!"),
        };

        bucket.query(statement, options)
    }

    pub fn disconnect(&mut self) {
        for bucket in self.buckets.values() {
            bucket.close();
        }
    }
}
