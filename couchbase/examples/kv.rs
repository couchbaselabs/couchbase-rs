use couchbase::*;
use futures::executor::block_on;
use std::collections::HashMap;

/// Key Value Examples.
///
/// This sample file shows how to do simple KV operations. You can see how each method
/// takes options (use the `::default()`) implementation if you do not need custom options)
/// and returns a `CouchbaseResult` that you need to match on.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("travel-sample");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let collection = bucket.default_collection();

    // Fetch a document
    match block_on(collection.get("airline_10", GetOptions::default())) {
        Ok(r) => println!("get result: {:?}", r),
        Err(e) => println!("get failed! {}", e),
    };

    // Upsert a document as JSON
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    match block_on(collection.upsert("foo", content, UpsertOptions::default())) {
        Ok(r) => println!("upsert result: {:?}", r),
        Err(e) => println!("upsert failed! {}", e),
    };

    // Remove a document
    match block_on(collection.remove("foo", RemoveOptions::default())) {
        Ok(r) => println!("remove result: {:?}", r),
        Err(e) => println!("remove failed! {}", e),
    }
}
