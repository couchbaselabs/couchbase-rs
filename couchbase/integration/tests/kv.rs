use couchbase::{
    GetOptions, GetSpecOptions, LookupInOptions, LookupInSpec, ReplaceOptions, UpsertOptions,
};

use crate::util::{BeerDocument, TestConfig};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{util, TestResult};
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

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

pub async fn test_upsert_preserve_expiry(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::PreserveExpiry) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let start = Local::now();
    let result = collection
        .upsert(
            &key,
            &doc,
            UpsertOptions::default().expiry(Duration::from_secs(25)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default().preserve_expiry(true))
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get(
                "$document.exptime",
                GetSpecOptions::default().xattr(true),
            )],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    let expiry_timestamp = result.content(0)?;
    let expires_at = NaiveDateTime::from_timestamp(expiry_timestamp, 0);
    assert!(
        start.signed_duration_since(DateTime::<Utc>::from_utc(expires_at, Utc))
            < chrono::Duration::from_std(Duration::from_secs(5)).unwrap()
    );

    Ok(false)
}

pub async fn test_replace_preserve_expiry(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::PreserveExpiry) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let start = Local::now();
    let result = collection
        .upsert(
            &key,
            &doc,
            UpsertOptions::default().expiry(Duration::from_secs(25)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default().preserve_expiry(true))
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get(
                "$document.exptime",
                GetSpecOptions::default().xattr(true),
            )],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    let expiry_timestamp = result.content(0)?;
    let expires_at = NaiveDateTime::from_timestamp(expiry_timestamp, 0);
    assert!(
        start.signed_duration_since(DateTime::<Utc>::from_utc(expires_at, Utc))
            < chrono::Duration::from_std(Duration::from_secs(5)).unwrap()
    );

    Ok(false)
}
