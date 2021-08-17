use async_std::task;
use couchbase::*;
use futures::stream::StreamExt;

fn main() {
    task::block_on(run())
}

async fn run() {
    let cluster = Cluster::connect("couchbase://localhost", "Administrator", "password");

    match cluster.query("select * from `travel-sample` limit 5", QueryOptions::default()).await {
        Ok(mut result) => {
            let mut rows = result.rows::<serde_json::Value>();
            while let Some(row) = rows.next().await {
                println!("Found Row {:?}", row);
            }
        }
        Err(e) => panic!("Query failed: {:?}", e),
    }
}
