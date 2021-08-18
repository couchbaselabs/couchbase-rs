use couchbase::{
    Cluster, CollectionSpec, CreateCollectionOptions, CreateScopeOptions, DropCollectionOptions,
    DropScopeOptions, GetAllScopesOptions,
};
use futures::executor::block_on;
use std::time::Duration;

/// Collections Examples
///
/// This file shows how to manage collections against the Cluster.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://10.112.210.101", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("default");

    // Create a user manager
    let manager = bucket.collections();

    match block_on(manager.create_scope("testest", CreateScopeOptions::default())) {
        Ok(_result) => {}
        Err(e) => println!("got error! {}", e),
    };

    match block_on(manager.create_collection(
        CollectionSpec::new("test2", "testest", Duration::from_secs(15)),
        CreateCollectionOptions::default(),
    )) {
        Ok(_result) => {}
        Err(e) => println!("got error! {}", e),
    };

    match block_on(manager.get_all_scopes(GetAllScopesOptions::default())) {
        Ok(result) => {
            println!("all scopes: {:?}", result);
        }
        Err(e) => println!("got error! {}", e),
    };

    match block_on(manager.drop_collection(
        CollectionSpec::new("test2", "testest", Duration::from_secs(0)),
        DropCollectionOptions::default(),
    )) {
        Ok(result) => {
            println!("all scopes: {:?}", result);
        }
        Err(e) => println!("got error! {}", e),
    };

    match block_on(manager.drop_scope("testest", DropScopeOptions::default())) {
        Ok(result) => {
            println!("all scopes: {:?}", result);
        }
        Err(e) => {
            println!("got error! {}", e)
        }
    };
}
