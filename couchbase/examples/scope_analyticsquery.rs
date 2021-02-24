use couchbase::{AnalyticsOptions, Cluster};
use futures::executor::{block_on, block_on_stream};
use serde_json::json;

/// Query Examples
///
/// This file shows how to perform analytics queries against a scope.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://10.112.210.101", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("default");

    let scope = bucket.scope("test");

    // Performs a simple query with no options and prints the results
    match block_on(scope.analytics_query(
        "select `test`.* from `test` limit 2",
        AnalyticsOptions::default(),
    )) {
        Ok(mut result) => {
            for row in block_on_stream(result.rows::<serde_json::Value>()) {
                println!("query row: {:?}", row);
            }

            println!("query meta: {:?}", block_on(result.meta_data()));
        }
        Err(e) => println!("got error! {}", e),
    }
}
