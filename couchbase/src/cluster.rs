//! Cluster-level operations and API.
//!
//! The Cluster structure is the main entrance point and acts as a factory for `Bucket` instances
//! which allow you to perform operations on the bucket itself.
//!
//! # Examples
//!
//! ```rust,no_run
//! use couchbase::Cluster;
//!
//! let _ = Cluster::new("localhost").expect("Could not initialize Cluster");
//! ```
//!
use Bucket;
use CouchbaseError;
use connstr::ConnectionString;

pub struct Cluster<'a> {
    connstr: &'a str,
    username: Option<&'a str>,
    password: Option<&'a str>,
}

impl<'a> Cluster<'a> {
    /// Creates a new `Cluster` instance.
    pub fn new(connstr: &'a str) -> Result<Self, CouchbaseError> {
        Ok(Cluster {
            connstr: connstr,
            username: None,
            password: None,
        })
    }

    pub fn authenticate(&mut self, username: &'a str, password: &'a str) {
        self.username = Some(username);
        self.password = Some(password);
    }

    /// Opens a `Bucket` and returns ownership of it to the caller.
    pub fn open_bucket(
        &self,
        name: &'a str,
        password: Option<&'a str>,
    ) -> Result<Bucket, CouchbaseError> {
        let connstr = ConnectionString::new(self.connstr)?;
        match self.username {
            Some(user) => {
                // rbac
                if password.is_some() {
                    panic!("Either username & password or a bucket password, but not both!");
                }
                Bucket::new(&connstr.export(name), self.password.unwrap(), Some(user))
            }
            None => {
                // bucket auth
                Bucket::new(&connstr.export(name), password.unwrap_or(""), None)
            }
        }
    }
}
