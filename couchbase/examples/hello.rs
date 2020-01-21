use couchbase::{Cluster, GetOptions, UpsertOptions};
use futures::executor::block_on;
use std::collections::HashMap;

pub fn main() {
    env_logger::init();

    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    let bucket = cluster.bucket("travel-sample");
    let collection = bucket.default_collection();

    let result = block_on(collection.get("airline_10", GetOptions::default()));
    println!("result: {:?}", result);
    match result {
        Ok(r) => println!("content: {:?}", r.content::<serde_json::Value>()),
        Err(e) => println!("got error! {:?}", e),
    };

    let mut content = HashMap::new();
    content.insert("Hello", "World1");

    println!("UpsertResult: {:?}", block_on(collection.upsert("foo", content, UpsertOptions::default())));
}
