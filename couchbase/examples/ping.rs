use couchbase::*;
use futures::executor::block_on;

/// Ping Examples.
///
/// This sample file shows how to do simple ping operations. You can see how each method
/// takes options (use the `::default()`) implementation if you do not need custom options)
/// and returns a `CouchbaseResult` that you need to match on.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://172.23.111.130", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("default");

    // Do a ping
    match block_on(bucket.ping(PingOptions::default())) {
        Ok(r) => println!("get result: {:?}", r),
        Err(e) => println!("get failed! {}", e),
    };
}
