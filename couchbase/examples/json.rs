extern crate couchbase;
extern crate futures;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use couchbase::Cluster;
use couchbase::document::{Document, JsonDocument};
use futures::executor::block_on;
use futures::FutureExt;

#[derive(Serialize, Deserialize, Debug)]
struct Airline {
    id: u32,
    #[serde(rename = "type")]
    _type: String,
    name: String,
    iata: String,
    icao: String,
    callsign: String,
    country: String,
}

/// A slightly more complicated example which loads a document and then relies on `serde`
/// JSON deserialization to marshal that document into a typed struct.
fn main() {
    // Initialize the Cluster
    let mut cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Use this for RBAC / Spock 5.0
    cluster.authenticate("Administrator", "password");

    // Open the travel-sample bucket
    let bucket = cluster
        .open_bucket("travel-sample", None)
        .expect("Could not open Bucket");

    let document: Airline = block_on(
        bucket
            .get::<JsonDocument<_>, _>("airline_10123")
            .map(|doc| doc.content().unwrap()),
    ).expect("Document not found!");

    println!("{:?}", document);
}
