use couchbase::*;
use futures::executor::block_on;

/// Key Value Subdoc Examples.
pub fn main() {
    env_logger::init();

    // Connect to the cluster with a connection string and credentials
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    // Open a bucket
    let bucket = cluster.bucket("travel-sample");
    // Use the default collection (needs to be used for all server 6.5 and earlier)
    let collection = bucket.default_collection();

    let lookup_in_result = block_on(collection.lookup_in(
        "airline_10",
        vec![LookupInSpec::get("country"), LookupInSpec::exists("iata")],
        LookupInOptions::default(),
    ));
    println!("{:?}", lookup_in_result);

    // MutateIn is an atomic operation. If any single operation fails,
    // then the entire document is left unchanged.
    let mutate_in_result = block_on(collection.mutate_in(
        "airline_10",
        vec![
            MutateInSpec::replace("name", "52-Mile Air"),
            MutateInSpec::upsert("foo", "bar"),
        ],
        MutateInOptions::default(),
    ));
    println!("{:?}", mutate_in_result);
}
