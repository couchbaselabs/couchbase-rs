use couchbase::Cluster;
use futures::Future;
use serde_json::Value;

fn main() {
    let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
        .expect("Could not create cluster reference!");
    let _ = cluster.bucket("travel-sample");

    let mut result = cluster
        .analytics_query("SELECT DataverseName FROM Metadata.`Dataverse`", None)
        .wait()
        .expect("Could not perform analytics query");

    println!("---> rows {:?}", result.rows_as().collect::<Vec<Value>>());
    println!("---> meta {:?}", result.meta());

    cluster.disconnect().expect("Could not shutdown properly");
}
