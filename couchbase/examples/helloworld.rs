extern crate couchbase;
extern crate futures;

use couchbase::{Cluster, Document};
use couchbase::document::BinaryDocument;
use futures::Future;

/// A very simple example which connects to the `default` bucket and writes and loads
/// a document.
fn main() {
    // Initialize the Cluster
    let mut cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // If you auth with 5.0 / RBAC, use this:
    cluster.authenticate("Administrator", "password");

    // Open the travel-sample bucket
    let bucket = cluster
        .open_bucket("default", None)
        .expect("Could not open Bucket");

    // Create a document and store it in the bucket
    let document = BinaryDocument::create("hello", None, Some("abc".as_bytes().to_owned()), None);
    println!(
        "Wrote Document {:?}",
        bucket.upsert(document).wait().expect("Upsert failed!")
    );

    // Load the previously written document and print it out
    let document: BinaryDocument = bucket.get("hello").wait().expect("Could not load Document");
    println!("Found Document {:?}", document);
}
