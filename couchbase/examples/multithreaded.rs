extern crate couchbase;
extern crate futures;

use std::sync::Arc;
use std::thread;

use couchbase::Cluster;
use futures::Future;

/// This example shows how to use the Bucket instance in a multithreaded context.
///
/// Threads are spawned and an `Arc` is used to share the Bucket (which is thread safe) across
/// them. Each thread then fetches a different airport document from the bucket and prints out
/// the results.
fn main() {
    // Initialize the Cluster
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = Arc::new(cluster.open_bucket("travel-sample", "").expect("Could not open Bucket"));

    let thread_count = 8;
    let mut threads = vec![];
    for i in 0..thread_count {
        let b = bucket.clone();
        threads.push(thread::spawn(move || {
            let id = format!("airport_{}", i + 1254);
            println!("Thread {:?} found:\n\t{:?}", i, b.get(&id).wait().unwrap());
        }));
    }

    for child in threads {
        let _ = child.join();
    }
}
