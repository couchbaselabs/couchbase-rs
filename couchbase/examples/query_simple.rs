use couchbase::{CouchbaseError, Cluster};
use futures::{Stream, Future};
use serde_json::Value;

fn main() {
    let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
        .expect("Could not create cluster reference");
    let _ = cluster.bucket("travel-sample");

    let mut result = cluster
        .query("select name, type from `travel-sample` limit 5", None)
        .wait()
        .expect("Could not perform query");

    println!("Rows:\n{:?}", result.rows_as().wait().collect::<Vec<Result<Value, CouchbaseError>>>());
    println!("Meta:\n{:?}", result.meta().wait().expect("Could not get query meta"));

    cluster.disconnect().expect("Could not shutdown properly");
}
