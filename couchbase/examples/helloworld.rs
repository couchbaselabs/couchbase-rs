extern crate couchbase;
extern crate futures;

use couchbase::Bucket;
use futures::Future;

fn main() {
    let bucket = Bucket::new("couchbase://localhost/travel-sample", "");

    println!("{:?}", bucket.get("airline_10123").wait().unwrap());
}
