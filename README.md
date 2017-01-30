# Couchbase Rust SDK
A brand new `libcouchbase`-based binding for [Rust](https://www.rust-lang.org).

# Building
There are two options to build the SDK (or better, how to link the underlying)
`libcouchbase` library. If you run with all the default settings and just issue
a `cargo build`, the build file will use [cmake](https://cmake.org/) to actually
compile the library and link it out of the source tree. This has the benefit that
the SDK can compile against a specific version, making sure to not use invalid APIs
across versions.

If you want to force using a different libcouchbase version you can either fork
the code and plug in a different source tree, but the other option is to install
one on your machine and make it discoverable via `pkg-config`. If you set the
`COUCHBASE_SYS_USE_PKG_CONFIG` environment variable the build file will try to
discover both the library and its header files through `pkg-config` and there is
also no need to compile libcouchbase when doing so.

# Examples

Note that to run all the examples you need to run at least Rust 1.15.0 since
it supports custom derive on stable which `serde` needs. Run with
`cargo run --example=helloworld`.

```rust
extern crate couchbase;
extern crate futures;

use couchbase::Cluster;
use futures::Future;

fn main() {
    // Initialize the Cluster
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = cluster.open_bucket("travel-sample", "").expect("Could not open Bucket");

    // Load an airline, wait for it to load and print it out
    let document = bucket.get("airline_10123").wait().expect("Could not load Document");
    println!("{:?}", document);
}
```
