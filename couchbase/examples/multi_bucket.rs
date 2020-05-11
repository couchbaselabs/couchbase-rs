use couchbase::*;
use futures::executor::{block_on, block_on_stream};

/// Note that this example will only run against 6.5 or later with
/// travel-sample and beer-sample installed.
fn main() {
    // Initialize the connection
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");

    // First run a cluster-level query without opening a bucket
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
    };

    println!("---");

    // Now, open two buckets and find docs from each
    let travel_sample = cluster.bucket("travel-sample");
    let ts_collectiom = travel_sample.default_collection();

    let beer_sample = cluster.bucket("beer-sample");
    let bs_collection = beer_sample.default_collection();

    match block_on(ts_collectiom.get("airline_10", GetOptions::default())) {
        Ok(r) => println!("get result: {:?}", r),
        Err(e) => println!("get failed! {}", e),
    };

    match block_on(bs_collection.get("21st_amendment_brewery_cafe", GetOptions::default())) {
        Ok(r) => println!("get result: {:?}", r),
        Err(e) => println!("get failed! {}", e),
    };

    println!("---");

    // Finally, run another query again
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
    };
}
