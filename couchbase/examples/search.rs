use couchbase::{Cluster, QueryStringQuery, SearchOptions};
use futures::executor::{block_on, block_on_stream};

/// Search Examples
///
/// This file shows how to perform search queries against the Cluster. Note that if you
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

    // Perform a simple search query
    match block_on(cluster.search_query(
        String::from("test"),
        QueryStringQuery::new(String::from("swanky")),
        SearchOptions::default(),
    )) {
        Ok(mut result) => {
            for row in block_on_stream(result.rows()) {
                println!("search row: {:?}", row);
            }

            println!("search meta: {:?}", block_on(result.meta_data()));
        }
        Err(e) => println!("got error! {}", e),
    }
}
