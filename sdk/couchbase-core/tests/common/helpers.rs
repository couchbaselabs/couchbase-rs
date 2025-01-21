use couchbase_core::agent::Agent;
use couchbase_core::crudoptions::GetCollectionIdOptions;
use couchbase_core::mgmtoptions::{CreateCollectionOptions, DeleteCollectionOptions};
use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

pub fn generate_key() -> Vec<u8> {
    rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}

pub fn generate_bytes_value(len: usize) -> Vec<u8> {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}

pub fn generate_string_value(len: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

pub async fn create_collection_and_wait_for_kv(
    agent: Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
    deadline: Instant,
) {
    agent
        .create_collection(&CreateCollectionOptions::new(
            bucket_name,
            scope_name,
            collection_name,
        ))
        .await
        .unwrap();

    let fut = || async {
        loop {
            let resp = agent
                .get_collection_id(GetCollectionIdOptions::new(scope_name, collection_name))
                .await;
            if resp.is_ok() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    timeout_at(deadline, fut()).await.unwrap();
}

pub async fn delete_collection_and_wait_for_kv(
    agent: Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
    deadline: Instant,
) {
    agent
        .delete_collection(&DeleteCollectionOptions::new(
            bucket_name,
            scope_name,
            collection_name,
        ))
        .await
        .unwrap();

    let fut = || async {
        loop {
            let resp = agent
                .get_collection_id(GetCollectionIdOptions::new(scope_name, collection_name))
                .await;
            if resp.is_err() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    timeout_at(deadline, fut()).await.unwrap();
}
