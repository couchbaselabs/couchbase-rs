use couchbase::*;
use env_logger::Env;
use futures::executor::block_on;
use std::collections::HashMap;

/// Key Value Examples.
///
/// This sample file shows how to do simple KV operations. You can see how each method
/// takes options (use the `::default()`) implementation if you do not need custom options)
/// and returns a `CouchbaseResult` that you need to match on.
pub fn main() {
    env_logger::init();

    let auth = PasswordAuthenticator::new("Administrator", "password");
    let opts = ClusterOptions::default()
        .security_config(SecurityOptions::default().trust_store_path("./path/to/my/ca.pem"))
        .authenticator(Box::new(auth));
    let cluster = Cluster::connect_with_options("couchbases://127.0.0.1", opts);
    // Open a bucket
    let bucket = cluster.bucket("default");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let collection = bucket.default_collection();

    // Fetch a document
    match block_on(collection.get("airline_10123", GetOptions::default())) {
        Ok(r) => println!("get result: {:?}", r),
        Err(e) => println!("get failed! {}", e),
    };

    // Upsert a document as JSON
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    match block_on(collection.replace("foo", content, ReplaceOptions::default())) {
        Ok(r) => println!("replace result: {:?}", r),
        Err(e) => println!("replace failed! {}", e),
    };

    // Remove a document
    match block_on(collection.remove("foo", RemoveOptions::default())) {
        Ok(r) => println!("remove result: {:?}", r),
        Err(e) => println!("remove failed! {}", e),
    }
}
