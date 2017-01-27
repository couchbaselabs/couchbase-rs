use ::Bucket;
use ::CouchbaseError;

pub struct Cluster<'a> {
    host: &'a str,
}

impl<'a> Cluster<'a> {
    pub fn new(host: &'a str) -> Result<Self, CouchbaseError> {
        Ok(Cluster { host: host })
    }

    pub fn open_bucket(&self, name: &'a str, password: &'a str) -> Result<Bucket, CouchbaseError> {
        let connstr = format!("couchbase://{}/{}", self.host, name);
        Bucket::new(&connstr, password)
    }
}
