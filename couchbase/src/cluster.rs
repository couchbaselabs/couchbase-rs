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

pub struct Cluster {
    connstr: String,
    username: Option<String>,
    password: Option<String>,
}

impl Cluster {
    /// Creates a new `Cluster` instance.
    pub fn new<S>(connstr: S) -> Result<Self, CouchbaseError>
    where
        S: Into<String>,
    {
        Ok(Cluster {
            connstr: connstr.into(),
            username: None,
            password: None,
        })
    }

    pub fn authenticate<S>(&mut self, username: S, password: S)
    where
        S: Into<String>,
    {
        self.username = Some(username.into());
        self.password = Some(password.into());
    }

    /// Opens a `Bucket` and returns ownership of it to the caller.
    pub fn open_bucket(
        &self,
        name: &str,
        password: Option<&str>,
    ) -> Result<Bucket, CouchbaseError> {
        let connstr = ConnectionString::new(&self.connstr)?;
        match self.username {
            Some(ref user) => {
                // rbac
                if password.is_some() {
                    panic!("Either username & password or a bucket password, but not both!");
                }
                Bucket::new(
                    &connstr.export(name),
                    self.password.as_ref().unwrap(),
                    Some(&user),
                )
            }
            None => {
                // bucket auth
                Bucket::new(&connstr.export(name), password.unwrap_or(""), None)
            }
        }
    }
}
