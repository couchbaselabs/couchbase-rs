extern crate couchbase;
extern crate futures;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use couchbase::Cluster;
use futures::Future;
use couchbase::document::{Document, JsonDocument};

#[derive(Serialize, Deserialize, Debug)]
struct Airline {
    id: u32,
    #[serde(rename = "type")] _type: String,
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
    let cluster = Cluster::new("localhost").expect("Could not initialize Cluster");

    // Open the travel-sample bucket
    let bucket = cluster
        .open_bucket("travel-sample", "")
        .expect("Could not open Bucket");

    let document: Airline = bucket
        .get::<JsonDocument<_>, _>("airline_10123")
        .map(|doc| doc.content().unwrap())
        .wait()
        .expect("Document not found!");

    println!("{:?}", document);
}
