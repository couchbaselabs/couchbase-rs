pub mod config;
pub mod features;
pub mod mock;
mod node_version;
pub mod collection;


use mock::MockCluster;

use crate::test::collection::TestResult;
use crate::test::collection::TestError;
pub use crate::test::features::TestFeature;
use crate::{Bucket, Cluster, Collection, Scope, UpsertOptions};
pub use config::{ClusterType, Config};
use log::warn;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

#[derive(Debug)]
pub struct TestConfig {
    cluster: Cluster,
    bucket: Bucket,
    scope: Scope,
    collection: Collection,
    support_matrix: Vec<TestFeature>,
    enabled_tests: Vec<String>,
}

impl TestConfig {
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }
    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn collection(&self) -> &Collection {
        &self.collection
    }
    pub fn supports_feature(&self, feature: TestFeature) -> bool {
        self.support_matrix.contains(&feature)
    }
    pub fn supports_features(&self, features: Vec<TestFeature>) -> bool {
        for feature in features {
            if !self.support_matrix.contains(&feature) {
                return false;
            }
        }

        true
    }
    pub fn test_enabled(&self, test: String) -> bool {
        if self.enabled_tests.is_empty() {
            return true;
        }

        self.enabled_tests.contains(&test)
    }
}



pub trait ConfigAware {
    fn config(&self) -> Arc<TestConfig>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BreweryDocumentGeo {
    pub accuracy: String,
    pub lat: f32,
    pub lon: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BreweryDocument {
    pub address: Vec<String>,
    pub city: String,
    pub code: String,
    pub description: String,
    pub geo: BreweryDocumentGeo,
    pub name: String,
    pub test_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct BeerDocument {
    pub brewery_id: String,
    pub category: String,
    pub description: String,
    pub ibu: u32,
    pub name: String,
    pub srm: u32,
    pub ingredients: Vec<String>,
    pub style: String,
}

pub async fn try_until<T, U>(deadline: Instant, future: impl Fn() -> T) -> TestResult<U>
    where
        T: Future<Output = TestResult<U>>,
{
    loop {
        if Instant::now() > deadline {
            return Err(TestError {
                reason: String::from("deadline exceeded during try_util"),
            });
        }

        match future().await {
            Ok(r) => return Ok(r),
            Err(e) => {
                warn!("Error received during try_until: {}", e);
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

pub async fn upsert_brewery_dataset(
    test_name: &str,
    collection: &Collection,
) -> TestResult<Vec<BreweryDocument>> {
    // TODO: this just feels wrong
    let dataset: Vec<BreweryDocument> = load_dataset("beer_sample_brewery_five.json")?;
    let mut result = vec![];
    for doc in &dataset {
        let mut new_doc = doc.clone();
        new_doc.test_name = Some(String::from(test_name));
        collection
            .upsert(&new_doc.name, &new_doc, UpsertOptions::default())
            .await?;
        result.push(new_doc);
    }

    Ok(result)
}

pub fn load_dataset<I, T>(file_path: impl Into<PathBuf>) -> TestResult<I>
    where
        I: IntoIterator<Item = T> + DeserializeOwned,
        T: DeserializeOwned,
{
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("integration");
    path.push("resources");
    path.push(file_path.into());

    Ok(serde_json::from_slice(
        fs::read(path)
            .map_err(|e| TestError {
                reason: e.to_string(),
            })?
            .as_slice(),
    )
        .map_err(|e| TestError {
            reason: e.to_string(),
        })?)
}

pub fn load_dataset_single<T>(file_path: impl Into<PathBuf>) -> TestResult<T>
    where
        T: DeserializeOwned,
{
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("integration");
    path.push("resources");
    path.push(file_path.into());

    Ok(serde_json::from_slice(
        fs::read(path)
            .map_err(|e| TestError {
                reason: e.to_string(),
            })?
            .as_slice(),
    )
        .map_err(|e| TestError {
            reason: e.to_string(),
        })?)
}
