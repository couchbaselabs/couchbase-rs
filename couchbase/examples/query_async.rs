use async_std::task;
use couchbase::*;
use futures::stream::StreamExt;

fn main() {
    task::block_on(run())
}

async fn run() {
    let cluster = Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");

    match cluster.query("select 1=1", QueryOptions::default()).await {
        Ok(mut result) => {
            for row in result.rows::<serde_json::Value>().next().await {
                println!("Found Row {:?}", row);
            }
        }
        Err(e) => panic!("Query failed: {:?}", e),
    }
}
