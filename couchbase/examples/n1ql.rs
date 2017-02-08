extern crate couchbase;
extern crate futures;

use couchbase::{N1qlResult, Cluster};
use futures::Stream;

/// A very simple example which connects to the `default` bucket and writes and loads
/// a document.
fn main() {
    // Initialize the Cluster
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = cluster.open_bucket("travel-sample", "").expect("Could not open Bucket");

    // Run the query and iterate the rows.
    for row in bucket.query_n1ql("SELECT count(*) as cnt FROM `travel-sample`").wait() {
        match row {
            Ok(N1qlResult::Row(r)) => println!("Found Row {:?}", r),
            Ok(N1qlResult::Meta(m)) => println!("Found Meta {:?}", m),
            Err(e) => panic!("Error! {:?}", e),
        }
    }

}
