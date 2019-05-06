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
        .expect("Could not create Cluster reference!");
    let bucket = cluster
        .bucket("travel-sample")
        .expect("Could not open bucket");
    let collection = bucket.default_collection();

    let found_doc = collection
        .get("airport_1297", None)
        .wait()
        .expect("Error while loading doc");
    println!("Airline Document: {:?}", found_doc);

    if found_doc.is_some() {
        println!(
            "Content Decoded {:?}",
            found_doc.unwrap().content_as::<Airport>()
        );
    }
    println!(
        "Document does exist?: {:?}",
        collection.exists("airport_1297", None).wait()
    );

    println!(
        "Airline Document: {:?}",
        collection.get("enoent", None).wait()
    );

    println!("Upsert: {:?}", collection.upsert("foo", "bar", None).wait());
    println!("Get: {:?}", collection.get("foo", None).wait());

    println!("Remove: {:?}", collection.remove("foo", None).wait());
    println!("Get: {:?}", collection.get("foo", None).wait());

    println!(
        "First Insert: {:?}",
        collection.insert("bla", "bla", None).wait()
    );
    println!(
        "Second Insert: {:?}",
        collection.insert("bla", "bla", None).wait()
    );

    cluster.disconnect().expect("Could not shutdown properly");
}
