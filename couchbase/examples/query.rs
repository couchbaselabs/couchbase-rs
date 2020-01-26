use couchbase::{Cluster, QueryOptions, QueryScanConsistency};
use futures::executor::{block_on, block_on_stream};

pub fn main() {
    env_logger::init();

    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    let bucket = cluster.bucket("travel-sample");
    let _collection = bucket.default_collection();

    let mut result = block_on(cluster.query(
        "select * from `travel-sample` limit 2",
        QueryOptions::default().scan_consistency(QueryScanConsistency::RequestPlus),
    ))
    .expect("Failed query");

    println!("result: {:?}", result);

    let meta = block_on(result.meta_data());
    println!("Metadata: {:?}", &meta);
    println!("{:?}", meta.metrics().elapsed_time());
    for row in block_on_stream(result.rows::<serde_json::Value>()) {
        println!("row: {:?}", row);
    }
}
