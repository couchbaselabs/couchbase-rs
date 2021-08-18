use couchbase::{CouchbaseError, QueryOptions};
use futures::stream::StreamExt;

mod util;

#[tokio::test]
async fn query() -> Result<(), CouchbaseError> {
    let cfg = util::setup().await;

    let cluster = cfg.cluster();

    let mut result = cluster.query("SELECT 1=1", QueryOptions::default()).await?;
    let mut num_rows: i32 = 0;
    let mut rows = result.rows::<serde_json::Value>();
    while let Some(_row) = rows.next().await {
        num_rows += 1;
    }

    assert_eq!(1, num_rows);

    Ok(())
}
