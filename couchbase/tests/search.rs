use couchbase::{CouchbaseError, GetOptions, UpsertOptions};

use crate::util::TestFeature;
use log::warn;
use std::collections::HashMap;
use uuid::Uuid;

mod util;

#[tokio::test]
async fn upsert_get() -> Result<(), CouchbaseError> {
    let cfg = util::setup().await;
    if !cfg.supports_feature(TestFeature::KeyValue) {
        warn!("Skipped...");
        return Ok(());
    }

    let collection = cfg.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(key.clone(), &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get(key, GetOptions::default()).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    Ok(())
}
