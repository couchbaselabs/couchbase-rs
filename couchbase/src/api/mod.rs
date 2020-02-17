pub mod error;
pub mod options;
pub mod results;

use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::options::*;
use crate::api::results::*;
use crate::io::request::*;
use crate::io::Core;
use futures::channel::oneshot;
use serde::Serialize;
use serde_json::{to_vec, Value};
use std::sync::Arc;
use std::time::Duration;

pub struct Cluster {
    core: Arc<Core>,
}

impl Cluster {
    pub fn connect<S: Into<String>>(connection_string: S, username: S, password: S) -> Self {
        Cluster {
            core: Arc::new(Core::new(
                connection_string.into(),
                username.into(),
                password.into(),
            )),
        }
    }

    pub fn bucket<S: Into<String>>(&self, name: S) -> Bucket {
        let name = name.into();
        self.core.open_bucket(name.clone());
        Bucket::new(self.core.clone(), name)
    }

    pub async fn query<S: Into<String>>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> CouchbaseResult<QueryResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options,
            sender,
        }));
        receiver.await.unwrap()
    }

    pub async fn analytics_query<S: Into<String>>(
        &self,
        statement: S,
        options: AnalyticsOptions,
    ) -> CouchbaseResult<AnalyticsResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options,
            sender,
        }));
        receiver.await.unwrap()
    }
}

pub struct Bucket {
    name: String,
    core: Arc<Core>,
}

impl Bucket {
    pub(crate) fn new(core: Arc<Core>, name: String) -> Self {
        Self { name, core }
    }

    pub fn default_collection(&self) -> Collection {
        Collection::new(self.core.clone(), "_default".into(), "_default".into())
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[cfg(feature = "volatile")]
    pub fn collection<S: Into<String>>(&self, name: S) -> Collection {
        Collection::new(self.core.clone(), name.into())
    }

    #[cfg(feature = "volatile")]
    pub fn scope<S: Into<String>>(&self, name: S) -> Scope {
        Scope::new(self.core.clone(), name.into())
    }
}

#[cfg(feature = "volatile")]
pub struct Scope {
    name: String,
    core: Arc<Core>,
}

#[cfg(feature = "volatile")]
impl Scope {
    pub(crate) fn new(core: Arc<Core>, name: String) -> Self {
        Self { core, name }
    }

    pub fn collection<S: Into<String>>(&self, name: S) -> Collection {
        Collection::new(self.core.clone(), name.into(), self.name.clone())
    }
}

pub struct Collection {
    core: Arc<Core>,
    name: String,
    _scope_name: String,
}

impl Collection {
    pub(crate) fn new(core: Arc<Core>, name: String, scope_name: String) -> Self {
        Self {
            core,
            name,
            _scope_name: scope_name,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn get<S: Into<String>>(
        &self,
        id: S,
        options: GetOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::Get { options },
            sender,
        }));
        receiver.await.unwrap()
    }

    pub async fn get_and_lock<S: Into<String>>(
        &self,
        id: S,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndLock { options, lock_time },
            sender,
        }));
        receiver.await.unwrap()
    }

    pub async fn get_and_touch<S: Into<String>>(
        &self,
        id: S,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndTouch { options, expiry },
            sender,
        }));
        receiver.await.unwrap()
    }

    pub async fn exists<S: Into<String>>(
        &self,
        id: S,
        options: ExistsOptions,
    ) -> CouchbaseResult<ExistsResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Exists(ExistsRequest {
            id: id.into(),
            options,
            sender,
        }));
        receiver.await.unwrap()
    }

    pub async fn upsert<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: UpsertOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Upsert { options })
            .await
    }

    pub async fn insert<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: InsertOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Insert { options })
            .await
    }

    pub async fn replace<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: ReplaceOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Replace { options })
            .await
    }

    async fn mutate<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        ty: MutateRequestType,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        let serialized = match to_vec(&content) {
            Ok(v) => v,
            Err(e) => {
                return Err(CouchbaseError::EncodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                })
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content: serialized,
            sender,
            ty,
        }));
        receiver.await.unwrap()
    }

    pub async fn remove<S: Into<String>>(
        &self,
        id: S,
        options: RemoveOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Remove(RemoveRequest {
            id: id.into(),
            sender,
            options,
        }));
        receiver.await.unwrap()
    }

    pub async fn lookup_in<S: Into<String>>(
        &self,
        id: S,
        specs: Vec<LookupInSpec>,
        options: LookupInOptions,
    ) -> CouchbaseResult<LookupInResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::LookupIn(LookupInRequest {
            id: id.into(),
            specs,
            sender,
            options,
        }));
        receiver.await.unwrap()
    }

    pub async fn mutate_in<S: Into<String>>(
        &self,
        id: S,
        specs: Vec<MutateInSpec>,
        options: MutateInOptions,
    ) -> CouchbaseResult<MutateInResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::MutateIn(MutateInRequest {
            id: id.into(),
            specs,
            sender,
            options,
        }));
        receiver.await.unwrap()
    }
}

#[derive(Debug)]
pub struct MutationState {
    tokens: Vec<MutationToken>,
}

#[derive(Debug)]
pub struct MutationToken {
    partition_uuid: u64,
    sequence_number: u64,
    partition_id: u16,
    bucket_name: String,
}

impl MutationToken {
    pub fn new(
        partition_uuid: u64,
        sequence_number: u64,
        partition_id: u16,
        bucket_name: String,
    ) -> Self {
        Self {
            partition_uuid,
            sequence_number,
            partition_id,
            bucket_name,
        }
    }

    pub fn partition_uuid(&self) -> u64 {
        self.partition_uuid
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn partition_id(&self) -> u16 {
        self.partition_id
    }

    pub fn bucket_name(&self) -> &String {
        &self.bucket_name
    }
}

#[derive(Debug)]
pub enum MutateInSpec {
    Replace { path: String, value: Vec<u8> },
    Insert { path: String, value: Vec<u8> },
    Upsert { path: String, value: Vec<u8> },
    ArrayAddUnique { path: String, value: Vec<u8> },
    Remove { path: String },
    Counter { path: String, delta: i64 },
    ArrayAppend { path: String, value: Vec<u8> },
    ArrayPrepend { path: String, value: Vec<u8> },
    ArrayInsert { path: String, value: Vec<u8> },
}

impl MutateInSpec {
    pub fn replace<S: Into<String>, T>(path: S, content: T) -> Self
    where
        T: Into<Value>,
    {
        let value = match to_vec(&content.into()) {
            Ok(v) => v,
            Err(_e) => panic!("Could not encode the value :-("),
        };
        MutateInSpec::Replace {
            path: path.into(),
            value,
        }
    }

    pub fn insert<S: Into<String>, T>(path: S, content: T) -> Self
    where
        T: Into<Value>,
    {
        let value = match to_vec(&content.into()) {
            Ok(v) => v,
            Err(_e) => panic!("Could not encode the value :-("),
        };
        MutateInSpec::Insert {
            path: path.into(),
            value,
        }
    }

    pub fn upsert<S: Into<String>, T>(path: S, content: T) -> Self
    where
        T: Into<Value>,
    {
        let value = match to_vec(&content.into()) {
            Ok(v) => v,
            Err(_e) => panic!("Could not encode the value :-("),
        };
        MutateInSpec::Upsert {
            path: path.into(),
            value,
        }
    }

    pub fn array_add_unique<S: Into<String>, T>(path: S, content: T) -> Self
    where
        T: Into<Value>,
    {
        let value = match to_vec(&content.into()) {
            Ok(v) => v,
            Err(_e) => panic!("Could not encode the value :-("),
        };
        MutateInSpec::ArrayAddUnique {
            path: path.into(),
            value,
        }
    }

    pub fn array_append<S: Into<String>, T>(path: S, content: Vec<T>) -> Self
    where
        T: Into<Value>,
    {
        let mut value = content
            .into_iter()
            .map(|v| {
                let mut encoded = match to_vec(&v.into()) {
                    Ok(v) => v,
                    Err(_e) => panic!("Could not encode the value :-("),
                };
                encoded.push(b',');
                encoded
            })
            .flatten()
            .collect::<Vec<_>>();
        value.pop().unwrap();

        MutateInSpec::ArrayAppend {
            path: path.into(),
            value,
        }
    }

    pub fn array_prepend<S: Into<String>, T>(path: S, content: Vec<T>) -> Self
    where
        T: Into<Value>,
    {
        let mut value = content
            .into_iter()
            .map(|v| {
                let mut encoded = match to_vec(&v.into()) {
                    Ok(v) => v,
                    Err(_e) => panic!("Could not encode the value :-("),
                };
                encoded.push(b',');
                encoded
            })
            .flatten()
            .collect::<Vec<_>>();
        value.pop().unwrap();

        MutateInSpec::ArrayPrepend {
            path: path.into(),
            value,
        }
    }

    pub fn array_insert<S: Into<String>, T>(path: S, content: Vec<T>) -> Self
    where
        T: Into<Value>,
    {
        let mut value = content
            .into_iter()
            .map(|v| {
                let mut encoded = match to_vec(&v.into()) {
                    Ok(v) => v,
                    Err(_e) => panic!("Could not encode the value :-("),
                };
                encoded.push(b',');
                encoded
            })
            .flatten()
            .collect::<Vec<_>>();
        value.pop().unwrap();

        MutateInSpec::ArrayInsert {
            path: path.into(),
            value,
        }
    }

    pub fn remove<S: Into<String>>(path: S) -> Self {
        MutateInSpec::Remove { path: path.into() }
    }

    pub fn increment<S: Into<String>>(path: S, delta: u32) -> Self {
        MutateInSpec::Counter {
            path: path.into(),
            delta: delta as i64,
        }
    }

    pub fn decrement<S: Into<String>>(path: S, delta: u32) -> Self {
        MutateInSpec::Counter {
            path: path.into(),
            delta: -(delta as i64),
        }
    }
}

#[derive(Debug)]
pub enum LookupInSpec {
    Get { path: String },
    Exists { path: String },
    Count { path: String },
}

impl LookupInSpec {
    pub fn get<S: Into<String>>(path: S) -> Self {
        LookupInSpec::Get { path: path.into() }
    }

    pub fn exists<S: Into<String>>(path: S) -> Self {
        LookupInSpec::Exists { path: path.into() }
    }

    pub fn count<S: Into<String>>(path: S) -> Self {
        LookupInSpec::Count { path: path.into() }
    }
}
