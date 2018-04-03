extern crate couchbase;
extern crate futures;

use couchbase::{Cluster, N1qlResult};
use futures::StreamExt;
use futures::executor::block_on;

/// Opens a bucket and then runs a N1QL query and prints out the results.
fn main() {
    // Initialize the Cluster
    let mut cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    cluster.authenticate("Administrator", "password");

    // Open the travel-sample bucket
    let bucket = cluster
        .open_bucket("travel-sample", None)
        .expect("Could not open Bucket");

    // Run the query and iterate the rows.
    let stream = bucket.query_n1ql("SELECT count(*) as cnt FROM `travel-sample`");
    for row in block_on(stream.collect::<Vec<_>>()).expect("Error!") {
        match row {
            N1qlResult::Row(r) => println!("Found Row {:?}", r),
            N1qlResult::Meta(m) => println!("Found Meta {:?}", m),
        }
    }
}
