use couchbase::{AnalyticsOptions, Cluster};
use futures::executor::{block_on, block_on_stream};

/// Analytics Examples
///
/// This file shows how to perform analytics queries against the Cluster. Note that if you
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

    // Perform a simple analytics query
    match block_on(cluster.analytics_query(r#"select 1=1"#, AnalyticsOptions::default())) {
        Ok(mut result) => {
            for row in block_on_stream(result.rows::<serde_json::Value>()) {
                println!("analytics row: {:?}", row);
            }

            println!("analytics meta: {:?}", block_on(result.meta_data()));
        }
        Err(e) => println!("got error! {}", e),
    }
}
