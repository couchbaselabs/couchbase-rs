use crate::tests::assert_timestamp;
use crate::util::{BeerDocument, TestConfig};
use crate::{util, TestResult};
use chrono::{NaiveDateTime, Utc};
use couchbase::{
    ArrayAppendSpecOptions, ArrayInsertSpecOptions, ArrayPrependSpecOptions, CouchbaseError,
    CouchbaseResult, CountSpecOptions, DecrementSpecOptions, ExistsSpecOptions, GetOptions,
    GetSpecOptions, IncrementSpecOptions, InsertSpecOptions, LookupInOptions, LookupInSpec,
    MutateInOptions, MutateInSpec, MutationMacro, RemoveSpecOptions, ReplaceSpecOptions,
    StoreSemantics, UpsertOptions, UpsertSpecOptions,
};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub async fn test_upsert_lookupin(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let result = collection
        .upsert(key.clone(), &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![
                LookupInSpec::get("name", GetSpecOptions::default()),
                LookupInSpec::get("description", GetSpecOptions::default()),
                LookupInSpec::exists("itdoesnt", ExistsSpecOptions::default()),
                LookupInSpec::exists("category", ExistsSpecOptions::default()),
                LookupInSpec::get("italsodoesnt", GetSpecOptions::default()),
                LookupInSpec::count("ingredients", CountSpecOptions::default()),
            ],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert!(!result.exists(2), "Expected field to not exist");
    assert!(result.exists(3), "Expected field to exist");
    assert!(
        !result.exists(8),
        "Expected field with invalid index to not exist"
    );

    let name: String = result.content(0)?;
    assert_eq!(doc.name, name);
    let desc: String = result.content(1)?;
    assert_eq!(doc.description, desc);

    let doesnt_exist_result: CouchbaseResult<BeerDocument> = result.content(4);
    assert!(doesnt_exist_result.is_err());
    match doesnt_exist_result.unwrap_err() {
        CouchbaseError::PathNotFound { .. } => {}
        _ => panic!("Expected path not found error"),
    }

    let count: u32 = result.content(5)?;
    assert_eq!(4, count);

    Ok(false)
}

pub async fn test_mutatein_basic(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let result = collection
        .upsert(key.clone(), &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let fish_name = "blobfish";
    let new_name = "a fishy beer";
    let new_style = "fishy tastes";
    let mutate_result = collection
        .mutate_in(
            &key,
            vec![
                MutateInSpec::insert("fish", &fish_name, InsertSpecOptions::default())?,
                MutateInSpec::upsert("name", &new_name, UpsertSpecOptions::default())?,
                MutateInSpec::upsert("newName", &new_name, UpsertSpecOptions::default())?,
                MutateInSpec::upsert("description", Value::Null, UpsertSpecOptions::default())?,
                MutateInSpec::replace("style", &new_style, ReplaceSpecOptions::default())?,
                MutateInSpec::replace("category", Value::Null, ReplaceSpecOptions::default())?,
                MutateInSpec::remove("ibu", RemoveSpecOptions::default())?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![
                LookupInSpec::get("fish", GetSpecOptions::default()),
                LookupInSpec::get("name", GetSpecOptions::default()),
                LookupInSpec::get("newName", GetSpecOptions::default()),
                LookupInSpec::get("description", GetSpecOptions::default()),
                LookupInSpec::get("style", GetSpecOptions::default()),
                LookupInSpec::get("category", GetSpecOptions::default()),
                LookupInSpec::exists("ibu", ExistsSpecOptions::default()),
            ],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert_eq!(String::from(fish_name), result.content::<String>(0)?);
    assert_eq!(String::from(new_name), result.content::<String>(1)?);
    assert_eq!(String::from(new_name), result.content::<String>(2)?);
    assert_eq!(Value::Null, result.content::<Value>(3)?);
    assert_eq!(String::from(new_style), result.content::<String>(4)?);
    assert_eq!(Value::Null, result.content::<Value>(5)?);
    assert_eq!(false, result.exists(6));

    Ok(false)
}

#[derive(Debug, Serialize, Deserialize)]
struct ArrayDoc {
    fish: Vec<String>,
}

pub async fn test_mutatein_arrays(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc = ArrayDoc { fish: vec![] };

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![
                MutateInSpec::array_append(
                    "fish",
                    vec!["clownfish"],
                    ArrayAppendSpecOptions::default(),
                )?,
                MutateInSpec::array_prepend(
                    "fish",
                    vec!["whaleshark"],
                    ArrayPrependSpecOptions::default(),
                )?,
                MutateInSpec::array_insert(
                    "fish[1]",
                    vec!["catfish"],
                    ArrayInsertSpecOptions::default(),
                )?,
                MutateInSpec::array_append(
                    "fish",
                    vec!["manta ray", "stingray"],
                    ArrayAppendSpecOptions::default(),
                )?,
                MutateInSpec::array_prepend(
                    "fish",
                    vec!["carp", "goldfish"],
                    ArrayPrependSpecOptions::default(),
                )?,
                MutateInSpec::array_insert(
                    "fish[1]",
                    vec!["eel", "stonefish"],
                    ArrayInsertSpecOptions::default(),
                )?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get("fish", GetSpecOptions::default())],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    let expected = vec![
        "carp",
        "eel",
        "stonefish",
        "goldfish",
        "whaleshark",
        "catfish",
        "clownfish",
        "manta ray",
        "stingray",
    ];
    let actual = result.content::<Vec<&str>>(0)?;

    assert_eq!(expected, actual);

    Ok(false)
}

#[derive(Debug, Serialize, Deserialize)]
struct CounterDoc {
    counter: u32,
}

pub async fn test_mutatein_counters(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc = CounterDoc { counter: 20 };

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![
                MutateInSpec::increment("counter", 10, IncrementSpecOptions::default())?,
                MutateInSpec::decrement("counter", 5, DecrementSpecOptions::default())?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get("counter", GetSpecOptions::default())],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert_eq!(25, result.content::<i32>(0)?);

    Ok(false)
}

pub async fn test_mutatein_blank_path_remove(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();
    let doc: BeerDocument = util::load_dataset_single("beer_sample_beer_single.json")?;

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![MutateInSpec::remove("", RemoveSpecOptions::default())?],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection.get(key, GetOptions::default()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        CouchbaseError::DocumentNotFound { .. } => {}
        _ => panic!("Expected document not found error"),
    }

    Ok(false)
}

pub async fn test_mutatein_blank_path_get(config: Arc<TestConfig>) -> TestResult<bool> {
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

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![
                MutateInSpec::insert(
                    "xattrpath",
                    "xattrvalue",
                    InsertSpecOptions::default().xattr(true),
                )?,
                MutateInSpec::replace("", &doc, ReplaceSpecOptions::default())?,
            ],
            MutateInOptions::default()
                .store_semantics(StoreSemantics::Upsert)
                .expiry(Duration::from_secs(20)),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![
                LookupInSpec::get("$document.exptime", GetSpecOptions::default().xattr(true)),
                LookupInSpec::get("", GetSpecOptions::default()),
            ],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert!(result.content::<u64>(0)? > 0);
    assert_eq!(doc, result.content::<BeerDocument>(1)?);

    Ok(false)
}

pub async fn test_xattrs(config: Arc<TestConfig>) -> TestResult<bool> {
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

    let result = collection
        .upsert(&key, &doc, UpsertOptions::default())
        .await?;
    assert_ne!(0, result.cas());

    let fish_name = "flounder";
    let mutate_result = collection
        .mutate_in(
            &key,
            vec![
                MutateInSpec::insert("fish", &fish_name, InsertSpecOptions::default().xattr(true))?,
                MutateInSpec::replace("", &doc, ReplaceSpecOptions::default())?,
            ],
            MutateInOptions::default().store_semantics(StoreSemantics::Upsert),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![
                LookupInSpec::get("fish", GetSpecOptions::default().xattr(true)),
                LookupInSpec::get("name", GetSpecOptions::default()),
            ],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert_eq!(fish_name, result.content::<&str>(0)?);
    assert_eq!(doc.name, result.content::<&str>(1)?);

    Ok(false)
}

pub async fn test_macros(config: Arc<TestConfig>) -> TestResult<bool> {
    if !config.supports_feature(util::TestFeature::KeyValue) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Subdoc) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::Xattrs) {
        return Ok(true);
    }
    if !config.supports_feature(util::TestFeature::ExpandMacros) {
        return Ok(true);
    }

    let collection = config.collection();
    let key = Uuid::new_v4().to_string();

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![MutateInSpec::insert(
                "caspath",
                MutationMacro::CAS,
                InsertSpecOptions::default(),
            )?],
            MutateInOptions::default().store_semantics(StoreSemantics::Upsert),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get(
                "caspath",
                GetSpecOptions::default().xattr(true),
            )],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert!(result.content::<&str>(0)?.starts_with("0x"));

    Ok(false)
}

pub async fn test_mutatein_preserve_expiry(config: Arc<TestConfig>) -> TestResult<bool> {
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

    let mutate_result = collection
        .mutate_in(
            &key,
            vec![MutateInSpec::upsert(
                "test",
                "test",
                UpsertSpecOptions::default(),
            )?],
            MutateInOptions::default().preserve_expiry(true),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

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
    assert_timestamp(start, duration, &expires_at, Duration::from_secs(5));

    Ok(false)
}
