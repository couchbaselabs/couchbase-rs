use crate::util::{BeerDocument, TestConfig};
use crate::{util, TestResult};
use couchbase::{
    CouchbaseError, CouchbaseResult, LookupInOptions, LookupInSpec, MutateInOptions, MutateInSpec,
    UpsertOptions,
};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
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
                LookupInSpec::get("name"),
                LookupInSpec::get("description"),
                LookupInSpec::exists("itdoesnt"),
                LookupInSpec::exists("category"),
                LookupInSpec::get("italsodoesnt"),
                LookupInSpec::count("ingredients"),
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
                MutateInSpec::insert("fish", &fish_name)?,
                MutateInSpec::upsert("name", &new_name)?,
                MutateInSpec::upsert("newName", &new_name)?,
                MutateInSpec::upsert("description", Value::Null)?,
                MutateInSpec::replace("style", &new_style)?,
                MutateInSpec::replace("category", Value::Null)?,
                MutateInSpec::remove("ibu")?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![
                LookupInSpec::get("fish"),
                LookupInSpec::get("name"),
                LookupInSpec::get("newName"),
                LookupInSpec::get("description"),
                LookupInSpec::get("style"),
                LookupInSpec::get("category"),
                LookupInSpec::exists("ibu"),
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
                MutateInSpec::array_append("fish", vec!["clownfish"])?,
                MutateInSpec::array_prepend("fish", vec!["whaleshark"])?,
                MutateInSpec::array_insert("fish[1]", vec!["catfish"])?,
                MutateInSpec::array_append("fish", vec!["manta ray", "stingray"])?,
                MutateInSpec::array_prepend("fish", vec!["carp", "goldfish"])?,
                MutateInSpec::array_insert("fish[1]", vec!["eel", "stonefish"])?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get("fish")],
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
                MutateInSpec::increment("counter", 10)?,
                MutateInSpec::decrement("counter", 5)?,
            ],
            MutateInOptions::default(),
        )
        .await?;
    assert_ne!(0, mutate_result.cas());

    let result = collection
        .lookup_in(
            key,
            vec![LookupInSpec::get("counter")],
            LookupInOptions::default(),
        )
        .await?;
    assert_ne!(0, result.cas());

    assert_eq!(25, result.content::<i32>(0)?);

    Ok(false)
}
