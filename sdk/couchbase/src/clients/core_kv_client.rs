use crate::clients::couchbase_core_kv_client::CouchbaseCoreKvClient;
use crate::error;
use crate::options::kv_binary_options::*;
use crate::options::kv_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::*;
use bytes::Bytes;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct CoreKvClient {
    backend: CoreKvClientBackend,
}

impl CoreKvClient {
    pub fn new(backend: CoreKvClientBackend) -> Self {
        Self { backend }
    }

    pub async fn upsert(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: UpsertOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.upsert(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.upsert(id, value, flags, options).await
            }
        }
    }

    pub async fn insert(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: InsertOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.insert(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.insert(id, value, flags, options).await
            }
        }
    }

    pub async fn replace(
        &self,
        id: String,
        value: Bytes,
        flags: u32,
        options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.replace(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.replace(id, value, flags, options).await
            }
        }
    }

    pub async fn remove(
        &self,
        id: String,
        options: RemoveOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.remove(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.remove(id, options).await
            }
        }
    }

    pub async fn get(&self, id: String, options: GetOptions) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get(id, options).await
            }
        }
    }

    pub async fn exists(&self, id: String, options: ExistsOptions) -> error::Result<ExistsResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.exists(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.exists(id, options).await
            }
        }
    }

    pub async fn get_and_touch(
        &self,
        id: String,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get_and_touch(id, expiry, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get_and_touch(id, expiry, options).await
            }
        }
    }

    pub async fn get_and_lock(
        &self,
        id: String,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get_and_lock(id, lock_time, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get_and_lock(id, lock_time, options).await
            }
        }
    }

    pub async fn unlock(&self, id: String, cas: u64, options: UnlockOptions) -> error::Result<()> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.unlock(id, cas, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.unlock(id, cas, options).await
            }
        }
    }

    pub async fn touch(
        &self,
        id: String,
        expiry: Duration,
        options: TouchOptions,
    ) -> error::Result<TouchResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.touch(id, expiry, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.touch(id, expiry, options).await
            }
        }
    }

    pub async fn append(
        &self,
        id: String,
        value: Vec<u8>,
        options: AppendOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.append(id, value, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.append(id, value, options).await
            }
        }
    }

    pub async fn prepend(
        &self,
        id: String,
        value: Vec<u8>,
        options: PrependOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.prepend(id, value, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.prepend(id, value, options).await
            }
        }
    }

    pub async fn increment(
        &self,
        id: String,
        options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.increment(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.increment(id, options).await
            }
        }
    }

    pub async fn decrement(
        &self,
        id: String,
        options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.decrement(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.decrement(id, options).await
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
pub(crate) struct Couchbase2CoreKvClient {}

impl Couchbase2CoreKvClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn upsert(
        &self,
        _id: String,
        _value: Bytes,
        _flags: u32,
        _options: impl Into<Option<UpsertOptions>>,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn insert(
        &self,
        _id: String,
        _value: Bytes,
        _flags: u32,
        _options: InsertOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn replace(
        &self,
        _id: String,
        _value: Bytes,
        _flags: u32,
        _options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn remove(
        &self,
        _id: String,
        _options: RemoveOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn get(&self, _id: String, _options: GetOptions) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn exists(
        &self,
        _id: String,
        _options: ExistsOptions,
    ) -> error::Result<ExistsResult> {
        unimplemented!()
    }

    pub async fn get_and_touch(
        &self,
        _id: String,
        _expiry: Duration,
        _options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn get_and_lock(
        &self,
        _id: String,
        _lock_time: Duration,
        _options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn unlock(
        &self,
        _id: String,
        _cas: u64,
        _options: UnlockOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn touch(
        &self,
        _id: String,
        _expiry: Duration,
        _options: TouchOptions,
    ) -> error::Result<TouchResult> {
        unimplemented!()
    }

    pub async fn append(
        &self,
        _id: String,
        _value: Vec<u8>,
        _options: AppendOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn prepend(
        &self,
        _id: String,
        _value: Vec<u8>,
        _options: PrependOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn increment(
        &self,
        _id: String,
        _options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        unimplemented!()
    }

    pub async fn decrement(
        &self,
        _id: String,
        _options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        unimplemented!()
    }
}
