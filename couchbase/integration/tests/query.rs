use crate::util::{try_until, upsert_brewery_dataset, BreweryDocument, TestConfig, TestFeature};
use couchbase::{CreatePrimaryQueryIndexOptions, QueryOptions};
use futures::StreamExt;

use crate::{TestError, TestResult};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub async fn test_query(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(TestFeature::Query) {
        return Ok(true);
    }

    upsert_brewery_dataset("test_query", config.collection()).await?;
    let cluster = config.cluster();

    let idx_mgr = cluster.query_indexes();
    idx_mgr
        .create_primary_index(
            config.bucket().name(),
            CreatePrimaryQueryIndexOptions::default().ignore_if_exists(true),
        )
        .await?;

    try_until(Instant::now().add(Duration::from_secs(10)), || async {
        let mut result = cluster
            .query(
                format!(
                    "SELECT {}.* FROM {} WHERE `test` = \"{}\"",
                    config.bucket().name(),
                    config.bucket().name(),
                    "test_query"
                ),
                QueryOptions::default(),
            )
            .await?;

        let mut docs: Vec<BreweryDocument> = vec![];
        let mut rows = result.rows::<BreweryDocument>();
        while let Some(row) = rows.next().await {
            docs.push(row?);
        }

        if docs.len() == 5 {
            Ok::<Vec<BreweryDocument>, TestError>(docs)
        } else {
            Err::<Vec<BreweryDocument>, TestError>(TestError {
                reason: format!("Expected 5 rows but got {}", docs.len()),
            })
        }
    })
    .await?;

    // assert_eq!(1, num_rows);

    Ok(false)
}
