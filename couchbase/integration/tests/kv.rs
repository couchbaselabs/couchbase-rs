use couchbase::{GetOptions, ReplaceOptions, UpsertOptions};

use crate::util::TestConfig;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{util, TestResult};
use std::sync::Arc;

pub async fn test_upsert_get(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
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

    Ok(false)
}

pub async fn test_upsert_replace_get(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(key.clone(), &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let new_content = HashMap::new();
    content.insert("Hello", "DifferentRust!");

    let result = collection
        .replace(key.clone(), &new_content, ReplaceOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get(key, GetOptions::default()).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(new_content, actual_content);

    Ok(false)
}
