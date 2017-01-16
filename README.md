# Couchbase Rust SDK
A brand new `libcouchbase` binding for [Rust](https://www.rust-lang.org).

Barely usable, please see the TODO.md for what needs to be done to get to a 0.1.

# Example

Here's the only thing that works right now ;-)

```rust
extern crate couchbase;
extern crate futures;

use couchbase::Cluster;
use futures::Future;

fn main() {
    // Open the Cluster Reference
    let mut cluster = Cluster::new("127.0.0.1");

    // Open the Bucket
    let bucket = cluster.open_bucket("beer-sample", "").expect("Could not connect to bucket!");

    // Load the Document and print it (returns a future!)
    let document = bucket.get("21st_amendment_brewery_cafe-21a_ipa");

    // Wait until the op is completed and print out the result
    println!("Loaded: {:?}", document.wait());

    // when cluster goes out of scope, calls "close" on all buckets it owns.
}


```
