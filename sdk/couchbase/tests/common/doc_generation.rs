use couchbase::collection::Collection;
use couchbase::results::kv_results::MutationResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub fn load_sample_beer_dataset(for_service: &str) -> Vec<TestBreweryDocumentJson> {
    let data = include_bytes!("../testdata/beer_sample_brewery_five.json");
    let mut docs: Vec<TestBreweryDocumentJson> = serde_json::from_slice(data).unwrap();
    for mut doc in docs.iter_mut() {
        doc.service = for_service.to_string();
    }

    docs
}

#[derive(Debug, Clone)]
pub struct TestMutationResult {
    pub mutation_result: MutationResult,
    pub doc: TestBreweryDocumentJson,
}

pub async fn import_sample_beer_dataset(
    for_service: &str,
    collection: &Collection,
) -> HashMap<String, TestMutationResult> {
    let docs = load_sample_beer_dataset(for_service);
    let mut results = HashMap::new();
    for doc in docs.iter() {
        let key = format!("{}-{}", &doc.service, &doc.name);
        let result = collection.upsert(key.clone(), doc, None).await.unwrap();
        results.insert(
            key,
            TestMutationResult {
                mutation_result: result,
                doc: doc.clone(),
            },
        );
    }

    results
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct TestBreweryDocumentJson {
    pub city: String,
    pub code: String,
    pub country: String,
    pub description: String,
    pub geo: TestBreweryGeoJson,
    pub name: String,
    pub phone: String,
    pub state: String,
    pub r#type: String,
    pub updated: String,
    pub website: String,
    #[serde(default)]
    pub service: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct TestBreweryGeoJson {
    pub accuracy: String,
    pub lat: f32,
    pub lon: f32,
}
