use couchbase::Cluster;
use serde_derive::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct Airport {
    airportname: String,
    icao: String,
}

fn main() {
    let mut cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    let _bucket = cluster.bucket("travel-sample");

    let mut result = cluster
        .analytics_query("SELECT DataverseName FROM Metadata.`Dataverse`", None)
        .expect("Could not perform analytics query");

    println!("---> rows {:?}", result.rows_as().collect::<Vec<Value>>());
    println!("---> meta {:?}", result.meta());

    std::thread::sleep(std::time::Duration::from_secs(100));
}
