use crate::util::{TestConfig, TestFeature};
use couchbase::{CouchbaseResult, QueryOptions};
use futures::StreamExt;

use std::sync::Arc;

pub async fn query(config: Arc<TestConfig>) -> CouchbaseResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    let cluster = config.cluster();

    let mut result = cluster.query("SELECT 1=1", QueryOptions::default()).await?;
    let mut num_rows: i32 = 0;
    let mut rows = result.rows::<serde_json::Value>();
    while let Some(_row) = rows.next().await {
        num_rows += 1;
    }

    assert_eq!(1, num_rows);

    Ok(false)
}
