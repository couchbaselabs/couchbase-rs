use couchbase_core::agent::Agent;
use couchbase_core::crudoptions::GetCollectionIdOptions;
use couchbase_core::error::{Error, ErrorKind};
use couchbase_core::memdx::error::ServerErrorKind;
use couchbase_core::mgmtoptions::{
    CreateCollectionOptions, CreateScopeOptions, DeleteCollectionOptions, DeleteScopeOptions,
    EnsureManifestOptions,
};
use couchbase_core::mgmtx::responses::{
    CreateCollectionResponse, CreateScopeResponse, DeleteCollectionResponse, DeleteScopeResponse,
};
use couchbase_core::{error, memdx};
use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

pub fn generate_key() -> Vec<u8> {
    generate_string_key().into_bytes()
}

pub fn generate_string_key() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect::<String>()
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
            if let Some(e) = resp.err() {
                if is_memdx_error(&e)
                    .unwrap()
                    .is_server_error_kind(ServerErrorKind::UnknownCollectionName)
                {
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    timeout_at(deadline, fut()).await.unwrap();
}

pub fn is_memdx_error(e: &Error) -> Option<&memdx::error::Error> {
    match e.kind() {
        ErrorKind::Memdx(err, ..) => Some(err),
        _ => None,
    }
}

pub async fn delete_scope(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
) -> error::Result<DeleteScopeResponse> {
    let opts = &DeleteScopeOptions::new(bucket_name, scope_name);
    agent.delete_scope(opts).await
}

pub async fn delete_collection(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) -> error::Result<DeleteCollectionResponse> {
    let opts = &DeleteCollectionOptions::new(bucket_name, scope_name, collection_name);
    agent.delete_collection(opts).await
}

pub async fn create_scope(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
) -> error::Result<CreateScopeResponse> {
    let opts = &CreateScopeOptions::new(bucket_name, scope_name);
    agent.create_scope(opts).await
}

pub async fn create_scope_and_ensure_exists(agent: &Agent, bucket_name: &str, scope_name: &str) {
    let res = create_scope(agent, bucket_name, scope_name).await.unwrap();

    agent
        .ensure_manifest(&EnsureManifestOptions::new(
            bucket_name,
            u64::from_str_radix(&res.manifest_uid, 16).unwrap(),
        ))
        .await
        .unwrap();
}

pub async fn create_collection(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) -> error::Result<CreateCollectionResponse> {
    let opts = &CreateCollectionOptions::new(bucket_name, scope_name, collection_name);
    agent.create_collection(opts).await
}

pub async fn create_collection_and_ensure_exists(
    agent: &Agent,
    bucket_name: &str,
    scope_name: &str,
    collection_name: &str,
) {
    let res = create_collection(agent, bucket_name, scope_name, collection_name)
        .await
        .unwrap();

    agent
        .ensure_manifest(&EnsureManifestOptions::new(
            bucket_name,
            u64::from_str_radix(&res.manifest_uid, 16).unwrap(),
        ))
        .await
        .unwrap();
}
