use couchbase::{Cluster, CreateQueryIndexOptions, GetAllQueryIndexOptions, CreatePrimaryQueryIndexOptions, DropQueryIndexOptions, DropPrimaryQueryIndexOptions, BuildDeferredQueryIndexOptions};
use futures::executor::block_on;

/// Query Index Examples
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://localhost", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("travel-sample");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let _collection = bucket.default_collection();

    let index_manager = cluster.query_indexes();
    // Creates a simple index as deferred
    match block_on(index_manager.create_index(
        "travel-sample",
        "example",
        vec!["`type`".into(), "country".into()],
        CreateQueryIndexOptions::default().deferred(true),
    )) {
        Ok(_result) => {
            println!("index created");
        }
        Err(e) => println!("got error! {}", e),
    }

    // Creates a named primary index
    match block_on(index_manager.create_primary_index(
        "travel-sample",
        CreatePrimaryQueryIndexOptions::default().index_name("my_primary"),
    )) {
        Ok(_result) => {
            println!("primary index created");
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_indexes("travel-sample", GetAllQueryIndexOptions::default())) {
        Ok(result) => {
            for index in result {
                println!("Got index {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    // Trigger the deferred index to build.
    match block_on(index_manager.build_deferred_indexes("travel-sample", BuildDeferredQueryIndexOptions::default())) {
        Ok(result) => {
            for index in result {
                println!("Triggered index {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_indexes("travel-sample", GetAllQueryIndexOptions::default())) {
        Ok(result) => {
            for index in result {
                println!("Got index {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }

    // Drops an index with no options
    match block_on(index_manager.drop_index(
        "travel-sample",
        "example",
        DropQueryIndexOptions::default(),
    )) {
        Ok(_result) => {
            println!("index dropped");
        }
        Err(e) => println!("got error! {}", e),
    }

    // Drops a named primary index
    match block_on(index_manager.drop_primary_index(
        "travel-sample",
        DropPrimaryQueryIndexOptions::default().index_name("my_primary"),
    )) {
        Ok(_result) => {
            println!("primary index dropped");
        }
        Err(e) => println!("got error! {}", e),
    }

    match block_on(index_manager.get_all_indexes("travel-sample", GetAllQueryIndexOptions::default())) {
        Ok(result) => {
            for index in result {
                println!("Got index {:?}", index);
            }
        }
        Err(e) => println!("got error! {}", e),
    }
}
