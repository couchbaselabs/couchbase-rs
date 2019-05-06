use couchbase::Cluster;
use futures::Future;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
struct Airport {
    airportname: String,
    icao: String,
}

fn main() {
    let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password")
        .expect("Could not create cluster reference");
    let _ = cluster.bucket("travel-sample");

    let mut result = cluster
        .query(
            "select airportname, icao from `travel-sample` where type = \"airport\" limit 2",
            None,
        )
        .wait()
        .expect("Could not perform query");

    println!("---> rows {:?}", result.rows_as().collect::<Vec<Airport>>());
    println!("---> meta {:?}", result.meta());

    cluster.disconnect().expect("Could not shutdown properly");
}
