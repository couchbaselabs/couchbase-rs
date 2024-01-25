use couchbase::{
    ClientVerifiedDurability, CouchbaseError, DurabilityLevel, GetAndLockOptions, GetOptions,
    GetSpecOptions, LookupInOptions, LookupInSpec, PersistTo, RemoveOptions, ReplaceOptions,
    ReplicateTo, UpsertOptions,
};

use crate::util::{BeerDocument, TestConfig};
use std::collections::HashMap;
use uuid::Uuid;

use crate::tests::assert_timestamp;
use crate::{util, TestResult};
use chrono::{NaiveDateTime, Utc};
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
        .upsert(&key, &content, UpsertOptions::default())
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
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let new_content = HashMap::new();
    content.insert("Hello", "DifferentRust!");

    let result = collection
        .replace(&key, &new_content, ReplaceOptions::default())
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

    let start = Utc::now();
    let duration = Duration::from_secs(25);
    let result = collection
        .upsert(&key, &doc, UpsertOptions::default().expiry(duration))
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
    //let expires_at = NaiveDateTime::from_timestamp_opt(expiry_timestamp, 0)
    //    .expect("Invalid timestamp");
    assert_timestamp(start, duration, &expires_at, Duration::from_secs(5));

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

    let start = Utc::now();
    let duration = Duration::from_secs(25);
    let result = collection
        .upsert(&key, &doc, UpsertOptions::default().expiry(duration))
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
    //let expires_at = NaiveDateTime::from_timestamp_opt(expiry_timestamp, 0)
    //    .expect("Invalid timestamp for expiry");
    assert_timestamp(start, duration, &expires_at, Duration::from_secs(5));

    Ok(false)
}

pub async fn test_get_with_expiry(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let start = Utc::now();
    let duration = Duration::from_secs(25);
    let result = collection
        .upsert(&key, &doc, UpsertOptions::default().expiry(duration))
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get(&key, GetOptions::default().with_expiry(true))
        .await?;

    assert_ne!(0, result.cas());

    let actual_doc = result.content::<BeerDocument>()?;

    assert_eq!(doc, actual_doc);

    let expiry_timestamp = result.expiry_time().unwrap();
    assert_timestamp(start, duration, expiry_timestamp, Duration::from_secs(5));

    Ok(false)
}

pub async fn test_get_non_existant(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();

    let result = collection.get(&key, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentNotFound { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_double_insert(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();

    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    collection.insert(&key, &content, None).await?;
    let result = collection.insert(&key, &content, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentExists { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_upsert_get_remove(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get(&key, GetOptions::default()).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    let result = collection.remove(&key, None).await?;
    assert_ne!(0, result.cas());

    let result = collection.get(&key, GetOptions::default()).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentNotFound { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_remove_with_cas(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());
    let upsert_cas = result.cas();

    let result = collection.exists(&key, None).await?;
    assert!(result.exists());
    assert_ne!(0, result.cas().unwrap());

    let result = collection
        .remove(&key, RemoveOptions::default().cas(12343534))
        .await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::CasMismatch { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    let result = collection.exists(&key, None).await?;
    assert!(result.exists());
    assert_ne!(0, result.cas().unwrap());

    let result = collection
        .remove(&key, RemoveOptions::default().cas(upsert_cas))
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.exists(&key, None).await?;
    assert!(!result.exists());
    assert!(result.cas().is_none());

    Ok(false)
}

pub async fn test_get_and_touch(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let start = Utc::now();
    let duration = Duration::from_secs(10);
    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_and_touch(&key, duration, None).await?;
    assert_ne!(0, result.cas());
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content.clone(), actual_content);

    let result = collection
        .get(&key, GetOptions::default().with_expiry(true))
        .await?;
    assert_ne!(0, result.cas());

    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    let expiry_timestamp = result.expiry_time().unwrap();
    assert_timestamp(start, duration, expiry_timestamp, Duration::from_secs(5));

    Ok(false)
}

pub async fn test_get_and_lock(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get_and_lock(&key, Duration::from_secs(2), None)
        .await?;
    assert_ne!(0, result.cas());
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content.clone(), actual_content);

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default().timeout(Duration::from_secs(1)),
        )
        .await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentLocked { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default().timeout(Duration::from_secs(3)),
        )
        .await?;
    assert_ne!(0, result.cas());

    Ok(false)
}

pub async fn test_unlock(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get_and_lock(&key, Duration::from_secs(1), None)
        .await?;
    assert_ne!(0, result.cas());
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content.clone(), actual_content);

    collection.unlock(&key, result.cas(), None).await?;

    let result = collection.upsert(&key, &content, None).await?;
    assert_ne!(0, result.cas());

    Ok(false)
}

pub async fn test_unlock_invalid_cas(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get_and_lock(&key, Duration::from_secs(1), None)
        .await?;
    assert_ne!(0, result.cas());
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content.clone(), actual_content);

    let result = collection.unlock(&key, result.cas() + 1, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentLocked { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_double_lock(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get_and_lock(&key, Duration::from_secs(1), None)
        .await?;
    assert_ne!(0, result.cas());
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content.clone(), actual_content);

    let result = collection
        .get_and_lock(
            &key,
            Duration::from_secs(1),
            GetAndLockOptions::default().timeout(Duration::from_secs(1)),
        )
        .await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentLocked { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_touch(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let start = Utc::now();
    let duration = Duration::from_secs(10);
    let result = collection
        .upsert(&key, &content, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.touch(&key, duration, None).await?;
    assert_ne!(0, result.cas());

    let result = collection
        .get(&key, GetOptions::default().with_expiry(true))
        .await?;
    assert_ne!(0, result.cas());

    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    let expiry_timestamp = result.expiry_time().unwrap();
    assert_timestamp(start, duration, expiry_timestamp, Duration::from_secs(5));

    Ok(false)
}

pub async fn test_replicate_to_get_any_replica(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_features(vec![
        util::TestFeature::KeyValue,
        util::TestFeature::Replicas,
    ]) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default()
                .durability(DurabilityLevel::ClientVerified(
                    ClientVerifiedDurability::default().replicate_to(ReplicateTo::One),
                ))
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_any_replica(key, None).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    Ok(false)
}

pub async fn test_persist_to_get_any_replica(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_features(vec![
        util::TestFeature::KeyValue,
        util::TestFeature::Replicas,
    ]) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default()
                .durability(DurabilityLevel::ClientVerified(
                    ClientVerifiedDurability::default().persist_to(PersistTo::One),
                ))
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_any_replica(key, None).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    Ok(false)
}

pub async fn test_durability_majority_get_any_replica(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_features(vec![
        util::TestFeature::KeyValue,
        util::TestFeature::Replicas,
        util::TestFeature::Durability,
    ]) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default()
                .durability(DurabilityLevel::Majority)
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_any_replica(&key, None).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    // let result = collection
    //     .remove(
    //         &key,
    //         RemoveOptions::default()
    //             .durability(DurabilityLevel::Majority)
    //             .timeout(Duration::from_secs(5)),
    //     )
    //     .await?;
    // assert_ne!(0, result.cas());
    //
    // let result = collection.get(&key, None).await;
    // assert!(result.is_err());
    //
    // let err = result.err().unwrap();
    //
    // match err {
    //     CouchbaseError::DocumentNotFound { .. } => {}
    //     _ => {
    //         panic!("Expected document not found error but was {}", err)
    //     }
    // }

    Ok(false)
}

pub async fn test_durability_persist_to_majority_get_any_replica(
    config: Arc<TestConfig>,
) -> TestResult<bool> {
    if !config.supports_features(vec![
        util::TestFeature::KeyValue,
        util::TestFeature::Replicas,
        util::TestFeature::Durability,
    ]) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default()
                .durability(DurabilityLevel::PersistToMajority)
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_any_replica(&key, None).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    let result = collection
        .remove(
            &key,
            RemoveOptions::default()
                .durability(DurabilityLevel::PersistToMajority)
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get(&key, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentNotFound { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}

pub async fn test_durability_majority_persist_on_master_get_any_replica(
    config: Arc<TestConfig>,
) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Replicas) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Durability) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let mut content = HashMap::new();
    content.insert("Hello", "Rust!");

    let result = collection
        .upsert(
            &key,
            &content,
            UpsertOptions::default()
                .durability(DurabilityLevel::MajorityAndPersistOnMaster)
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get_any_replica(&key, None).await?;
    let actual_content: HashMap<&str, &str> = result.content()?;
    assert_eq!(content, actual_content);

    let result = collection
        .remove(
            &key,
            RemoveOptions::default()
                .durability(DurabilityLevel::MajorityAndPersistOnMaster)
                .timeout(Duration::from_secs(5)),
        )
        .await?;
    assert_ne!(0, result.cas());

    let result = collection.get(&key, None).await;
    assert!(result.is_err());

    let err = result.err().unwrap();

    match err {
        CouchbaseError::DocumentNotFound { .. } => {}
        _ => {
            panic!("Expected document not found error but was {}", err)
        }
    }

    Ok(false)
}
