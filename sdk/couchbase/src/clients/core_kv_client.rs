use crate::error;
use crate::results::get_result::GetResult;
use bytes::Bytes;
use couchbase_core::agent::Agent;

#[derive(Clone)]
pub(crate) struct CoreKvClient {
    backend: CoreKvClientBackend,
}

impl CoreKvClient {
    pub fn new(backend: CoreKvClientBackend) -> Self {
        Self { backend }
    }

    pub async fn upsert(&self, id: String, value: Bytes, flags: u32) -> error::Result<()> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.upsert(id, value, flags).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.upsert(id, value, flags).await
            }
        }
    }

    pub async fn get(&self, id: String) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => client.get(id).await,
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => client.get(id).await,
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

    pub async fn upsert(&self, id: String, value: Bytes, flags: u32) -> error::Result<()> {
        self.agent
            .upsert(
                couchbase_core::crudoptions::UpsertOptions::builder()
                    .key(id.as_bytes())
                    .value(&value)
                    .flags(flags)
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .build(),
            )
            .await?;

        Ok(())
    }

    pub async fn get(&self, id: String) -> error::Result<GetResult> {
        let res = self
            .agent
            .get(
                couchbase_core::crudoptions::GetOptions::builder()
                    .key(id.as_bytes())
                    .scope_name(&self.scope_name)
                    .collection_name(&self.collection_name)
                    .build(),
            )
            .await?;

        Ok(res.into())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2CoreKvClient {}

impl Couchbase2CoreKvClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn upsert(&self, _id: String, _value: Bytes, _flags: u32) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn get(&self, _id: String) -> error::Result<GetResult> {
        unimplemented!()
    }
}
