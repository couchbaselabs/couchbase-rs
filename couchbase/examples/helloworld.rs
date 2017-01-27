extern crate couchbase;
extern crate futures;

use couchbase::{Document, Cluster};
use futures::Future;

/// A very simple example which connects to the `default` bucket and writes and loads
/// a document.
fn main() {
    // Initialize the Cluster
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = cluster.open_bucket("default", "").expect("Could not open Bucket");

    // Create a document and store it in the bucket
    let document = Document::from_str_with_expiry("hello", "{\"world\":true}");
    println!("Wrote Document {:?}",
             bucket.upsert(document)
                 .wait()
                 .unwrap());

    // Load the previously written document and print it out
    match bucket.get("hello").wait().expect("Could not load Document") {
        Some(d) => println!("Found Document {:?}", d),
        None => println!("Document not found!"),
    }

}
