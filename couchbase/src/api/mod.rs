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
use serde_json::to_vec;
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
}

impl MutationToken {
    pub fn new(partition_uuid: u64, sequence_number: u64, partition_id: u16) -> Self {
        Self {
            partition_uuid,
            sequence_number,
            partition_id,
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
        todo!()
    }
}
