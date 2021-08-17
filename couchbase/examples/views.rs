use couchbase::{Cluster, CouchbaseResult, ViewOptions};
use futures::executor::{block_on, block_on_stream};

/// Query Examples
///
/// This file shows how to perform N1QL queries against the Cluster. Note that if you
/// are using a cluster pre 6.5 you need to open at least one bucket to make cluster-level
/// operations.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("travel-sample");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let _collection = bucket.default_collection();

    // Performs a simple query with no options and prints the results
    match block_on(bucket.view_query(
        "dev_test_ddoc",
        "test_view",
        ViewOptions::default().limit(5).key("airline_659"),
    )) {
        Ok(mut result) => {
            for row in block_on_stream(result.rows()) {
                println!("view row: {:?}", row);
                let row = row.unwrap();
                let key: CouchbaseResult<String> = row.key();
                println!("view key {:?}", key);
                let value: CouchbaseResult<String> = row.value();
                println!("view value {:?}", value);
                let id = row.id();
                println!("view id {:?}", id);
            }

            println!("view meta: {:?}", block_on(result.meta_data()));
        }
        Err(e) => println!("got error! {}", e),
    }
}
