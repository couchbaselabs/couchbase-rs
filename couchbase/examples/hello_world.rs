use couchbase::Cluster;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
struct Airport {
    airportname: String,
    icao: String,
}

fn main() {
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    let bucket = cluster.bucket("travel-sample");
    let collection = bucket.default_collection();

    let found_doc = collection
        .get("airport_1297", None)
        .expect("Error while loading doc");
    println!("Airline Document: {:?}", found_doc);

    if found_doc.is_some() {
        println!(
            "Content Decoded {:?}",
            found_doc.unwrap().content_as::<Airport>()
        );
    }

    println!("Airline Document: {:?}", collection.get("enoent", None));

    println!("{:?}", collection.upsert("foo", "bar", None));
    println!("{:?}", collection.get("foo", None));

    println!("{:?}", collection.remove("foo", None));
    println!("{:?}", collection.get("foo", None));

    std::thread::sleep(std::time::Duration::from_secs(100));
}
