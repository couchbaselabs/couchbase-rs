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
use ::Bucket;
use ::CouchbaseError;

pub struct Cluster<'a> {
    host: &'a str,
}

impl<'a> Cluster<'a> {
    /// Creates a new `Cluster` instance.
    pub fn new(host: &'a str) -> Result<Self, CouchbaseError> {
        Ok(Cluster { host: host })
    }

    /// Opens a `Bucket` and returns ownership of it to the caller.
    pub fn open_bucket(&self, name: &'a str, password: &'a str) -> Result<Bucket, CouchbaseError> {
        let connstr = format!("couchbase://{}/{}", self.host, name);
        Bucket::new(&connstr, password)
    }
}
