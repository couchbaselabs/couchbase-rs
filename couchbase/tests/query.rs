use crate::util::TestFeature;
use couchbase::{CouchbaseError, QueryOptions};
use futures::stream::StreamExt;
use log::warn;

mod util;

#[tokio::test]
async fn query() -> Result<(), CouchbaseError> {
    let cfg = util::setup().await;
    if !cfg.supports_feature(TestFeature::Query) {
        warn!("Skipped...");
        return Ok(());
    }

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
