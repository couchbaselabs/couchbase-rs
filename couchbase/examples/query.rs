use couchbase::{Cluster, QueryOptions};
use futures::executor::{block_on, block_on_stream};
use serde_json::json;

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
    match block_on(cluster.query(
        "select * from `travel-sample` limit 2",
        QueryOptions::default(),
    )) {
        Ok(mut result) => {
            for row in block_on_stream(result.rows::<serde_json::Value>()) {
                println!("query row: {:?}", row);
            }

            println!("query meta: {:?}", block_on(result.meta_data()));
        }
        Err(e) => println!("got error! {}", e),
    }

    // Performs a query with named parameters and prints the results

    let options = QueryOptions::default().named_parameters(json!({"type": "airport"}));
    let mut result = block_on(cluster.query(
        "select count(*) as count from `travel-sample` where type = $type",
        options,
    ))
    .expect("Named query failed!");
    for row in block_on_stream(result.rows::<serde_json::Value>()) {
        println!("named query row: {:?}", row);
    }
}
