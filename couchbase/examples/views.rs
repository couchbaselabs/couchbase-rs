extern crate couchbase;
extern crate futures;

use couchbase::{ViewResult, Cluster};
use futures::Stream;

/// A very simple example which connects to the `default` bucket and writes and loads
/// a document.
fn main() {
    // Initialize the Cluster
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = cluster.open_bucket("beer-sample", "").expect("Could not open Bucket");

    // Run the query and iterate the rows.
    for row in bucket.query_view("beer", "brewery_beers", "limit=-1").wait() {
        match row {
            Ok(ViewResult::Row(r)) => println!("Found Row {:?}", r),
            Ok(ViewResult::Meta(m)) => println!("Found Meta {:?}", m),
            Err(e) => panic!("Error! {:?}", e),
        }
    }
}
