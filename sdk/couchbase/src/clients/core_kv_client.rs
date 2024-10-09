use crate::error;
use couchbase_core::agent::Agent;
use serde::Serialize;

#[derive(Clone)]
pub(crate) struct CoreKvClient {
    backend: CoreKvClientBackend,
}

impl CoreKvClient {
    pub fn new(backend: CoreKvClientBackend) -> Self {
        Self { backend }
    }

    pub async fn upsert<T: Serialize>(&self, id: String, value: T) -> error::Result<()> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.upsert(id, value).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.upsert(id, value).await
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum CoreKvClientBackend {
    CouchbaseCoreKvClientBackend(CouchbaseCoreKvClient),
    Couchbase2CoreKvClientBackend(Couchbase2CoreKvClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseCoreKvClient {
    agent: Agent,
    bucket_name: String,
    scope_name: String,
    collection_name: String,
}

impl CouchbaseCoreKvClient {
    pub fn new(
        agent: Agent,
        bucket_name: String,
        scope_name: String,
        collection_name: String,
    ) -> Self {
        Self {
            agent,
            bucket_name,
            scope_name,
            collection_name,
        }
    }

    pub async fn upsert<T: Serialize>(&self, id: String, _value: T) -> error::Result<()> {
        self.agent
            .upsert(
                couchbase_core::crudoptions::UpsertOptions::builder()
                    .key(id.as_bytes())
                    .value(&[])
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .build(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2CoreKvClient {}

impl Couchbase2CoreKvClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn upsert<T: Serialize>(&self, _id: String, _value: T) -> error::Result<()> {
        unimplemented!()
    }
}
