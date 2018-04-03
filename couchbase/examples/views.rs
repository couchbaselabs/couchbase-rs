extern crate couchbase;
extern crate futures;

use couchbase::{Cluster, ViewQuery, ViewResult};
use futures::StreamExt;
use futures::executor::block_on;

/// Opens a bucket and then runs a view query and prints out the results.
fn main() {
    // Initialize the Cluster
    let mut cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    cluster.authenticate("Administrator", "password");

    // Open the travel-sample bucket
    let bucket = cluster
        .open_bucket("beer-sample", None)
        .expect("Could not open Bucket");

    // Run the query and iterate the rows.
    let stream = bucket.query_view(ViewQuery::from("beer", "brewery_beers").limit(3));
    for row in block_on(stream.collect::<Vec<_>>()).unwrap() {
        match row {
            ViewResult::Row(r) => println!("Found Row {:?}", r),
            ViewResult::Meta(m) => println!("Found Meta {:?}", m),
        }
    }
}
