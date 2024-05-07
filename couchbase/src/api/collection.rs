use super::*;
use crate::api::keyvalue_options::*;
use crate::api::keyvalue_results::*;
use crate::api::subdoc::*;
use crate::api::subdoc_options::*;
use crate::api::subdoc_results::*;
use crate::io::request::*;
use crate::io::LOOKUPIN_MACRO_EXPIRYTIME;
use crate::CouchbaseError::Generic;
use crate::{BinaryCollection, CouchbaseError, CouchbaseResult, ErrorContext};
use chrono::NaiveDateTime;
use futures::channel::oneshot;
use futures::FutureExt;
use futures::{pin_mut, select};
use serde::Serialize;
use serde_json::to_vec;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
/// Primary API to access Key/Value operations
#[derive(Debug)]
pub struct Collection {
    core: Arc<Core>,
    name: String,
    scope_name: String,
    bucket_name: String,
}

impl Collection {
    pub(crate) fn new(
        core: Arc<Core>,
        name: String,
        scope_name: String,
        bucket_name: String,
    ) -> Self {
        Self {
            core,
            name,
            scope_name,
            bucket_name,
        }
    }

    /// The name of the collection
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn get(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<GetOptions>>,
    ) -> CouchbaseResult<GetResult> {
        let options = unwrap_or_default!(options.into());
        if options.with_expiry {
            return self.get_with_expiry(id).await;
        }
        return self.get_direct(id, options).await;
    }

    async fn get_with_expiry(&self, id: impl Into<String>) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();

        // TODO: stuff with flags once supported
        let specs = vec![
            LookupInSpec::get(
                LOOKUPIN_MACRO_EXPIRYTIME,
                GetSpecOptions::default().xattr(true),
            ),
            LookupInSpec::get("", GetSpecOptions::default()),
        ];

        self.core.send(Request::LookupIn(LookupInRequest {
            id: id.into(),
            specs,
            sender,
            bucket: self.bucket_name.clone(),
            options: LookupInOptions::default(),
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        let lookup_result = receiver.await.unwrap()?;

        let expiry = NaiveDateTime::from_timestamp(lookup_result.content::<i64>(0)?, 0);
        let content = lookup_result.raw(1)?.to_vec();

        let mut result = GetResult::new(content, lookup_result.cas(), 0);
        result.set_expiry_time(expiry);

        Ok(result)
    }

    async fn get_direct(
        &self,
        id: impl Into<String>,
        options: GetOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::Get { options },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn get_any_replica(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<GetAnyReplicaOptions>>,
    ) -> CouchbaseResult<GetReplicaResult> {
        let options = unwrap_or_default!(options.into());
        let (get_sender, get_receiver) = oneshot::channel();
        let (get_replica_sender, get_replica_receiver) = oneshot::channel();
        let id = id.into();
        self.core.send(Request::GetReplica(GetReplicaRequest {
            id: id.clone(),
            options: GetReplicaOptions {
                timeout: options.timeout,
            },
            bucket: self.bucket_name.clone(),
            sender: get_replica_sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
            mode: ReplicaMode::Any,
        }));
        self.core.send(Request::Get(GetRequest {
            id,
            bucket: self.bucket_name.clone(),
            sender: get_sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
            ty: GetRequestType::Get {
                options: GetOptions {
                    timeout: options.timeout,
                    with_expiry: false,
                },
            },
        }));

        let get_receiver = get_receiver.fuse();
        let get_replica_receiver = get_replica_receiver.fuse();

        pin_mut!(get_receiver, get_replica_receiver);

        // TODO: not sure this will cancel the uncompleted future.
        loop {
            let result = select! {
                res = get_receiver=> {
                    match res.unwrap() {
                        Ok(gr) => Ok(GetReplicaResult::new(gr.content, gr.cas, gr.flags, false)),
                        Err(e) => {Err(e)}
                    }
                },
                res = get_replica_receiver => {
                    match res.unwrap() {
                        Ok(gr) => Ok(GetReplicaResult::new(gr.content, gr.cas, gr.flags, false)),
                        Err(e) => {Err(e)}
                    }
                }
                complete => {
                    return Err(CouchbaseError::DocumentNotFound {ctx: ErrorContext::default()})
                }
            };

            if result.is_ok() {
                return result;
            }
        }
    }

    pub async fn get_and_lock(
        &self,
        id: impl Into<String>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> CouchbaseResult<GetResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndLock { options, lock_time },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn get_and_touch(
        &self,
        id: impl Into<String>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> CouchbaseResult<GetResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndTouch { options, expiry },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn exists(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> CouchbaseResult<ExistsResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Exists(ExistsRequest {
            id: id.into(),
            options,
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn upsert<T>(
        &self,
        id: impl Into<String>,
        content: T,
        options: impl Into<Option<UpsertOptions>>,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        let options = unwrap_or_default!(options.into());
        self.mutate(id, content, MutateRequestType::Upsert { options })
            .await
    }

    pub async fn insert<T>(
        &self,
        id: impl Into<String>,
        content: T,
        options: impl Into<Option<InsertOptions>>,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        let options = unwrap_or_default!(options.into());
        self.mutate(id, content, MutateRequestType::Insert { options })
            .await
    }

    pub async fn replace<T>(
        &self,
        id: impl Into<String>,
        content: T,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        let options = unwrap_or_default!(options.into());
        self.mutate(id, content, MutateRequestType::Replace { options })
            .await
    }

    async fn mutate<T>(
        &self,
        id: impl Into<String>,
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
            bucket: self.bucket_name.clone(),
            ty,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn remove(
        &self,
        id: impl Into<String>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> CouchbaseResult<MutationResult> {
        let options = unwrap_or_default!(options.into());

        // lcb doesn't support observe based durability for remove.
        if let Some(durability) = options.durability {
            match durability {
                DurabilityLevel::ClientVerified(_) => {
                    return Err(CouchbaseError::InvalidArgument {
                        ctx: ErrorContext::from((
                            "durability",
                            "cannot use client verified durability with remove",
                        )),
                    })
                }
                _ => {}
            }
        }

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Remove(RemoveRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn touch(
        &self,
        id: impl Into<String>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> CouchbaseResult<MutationResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Touch(TouchRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
            expiry,
        }));
        receiver.await.unwrap()
    }

    pub async fn unlock(
        &self,
        id: impl Into<String>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> CouchbaseResult<()> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Unlock(UnlockRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
            cas,
        }));
        receiver.await.unwrap()
    }

    pub async fn lookup_in(
        &self,
        id: impl Into<String>,
        specs: impl IntoIterator<Item = LookupInSpec>,
        options: impl Into<Option<LookupInOptions>>,
    ) -> CouchbaseResult<LookupInResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::LookupIn(LookupInRequest {
            id: id.into(),
            specs: specs.into_iter().collect::<Vec<LookupInSpec>>(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn mutate_in(
        &self,
        id: impl Into<String>,
        specs: impl IntoIterator<Item = MutateInSpec>,
        options: impl Into<Option<MutateInOptions>>,
    ) -> CouchbaseResult<MutateInResult> {
        let options = unwrap_or_default!(options.into());
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::MutateIn(MutateInRequest {
            id: id.into(),
            specs: specs.into_iter().collect::<Vec<MutateInSpec>>(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(
            self.core.clone(),
            self.name.clone(),
            self.scope_name.clone(),
            self.bucket_name.clone(),
        )
    }
}

#[derive(Debug)]
pub struct MutationState {
    pub(crate) tokens: Vec<MutationToken>,
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

#[derive(Debug, Copy, Clone)]
pub enum PersistTo {
    One,
    Two,
    Three,
    Four,
}

impl From<PersistTo> for i32 {
    fn from(pt: PersistTo) -> Self {
        match pt {
            PersistTo::One => 1,
            PersistTo::Two => 2,
            PersistTo::Three => 3,
            PersistTo::Four => 4,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ReplicateTo {
    One,
    Two,
    Three,
}

impl From<ReplicateTo> for i32 {
    fn from(rt: ReplicateTo) -> Self {
        match rt {
            ReplicateTo::One => 1,
            ReplicateTo::Two => 2,
            ReplicateTo::Three => 3,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ClientVerifiedDurability {
    pub(crate) persist_to: Option<PersistTo>,
    pub(crate) replicate_to: Option<ReplicateTo>,
}

impl Default for ClientVerifiedDurability {
    fn default() -> Self {
        Self {
            persist_to: None,
            replicate_to: None,
        }
    }
}

impl ClientVerifiedDurability {
    pub fn new(
        persist_to: impl Into<Option<PersistTo>>,
        replicate_to: impl Into<Option<ReplicateTo>>,
    ) -> Self {
        Self {
            persist_to: persist_to.into(),
            replicate_to: replicate_to.into(),
        }
    }
    pub fn persist_to(mut self, persist_to: PersistTo) -> Self {
        self.persist_to = Some(persist_to);
        self
    }
    pub fn replicate_to(mut self, replicate_to: ReplicateTo) -> Self {
        self.replicate_to = Some(replicate_to);
        self
    }
}

#[derive(Debug, Copy, Clone)]
pub enum DurabilityLevel {
    None,
    Majority,
    MajorityAndPersistOnMaster,
    PersistToMajority,
    ClientVerified(ClientVerifiedDurability),
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        DurabilityLevel::None
    }
}

impl Display for DurabilityLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            DurabilityLevel::None => "none",
            DurabilityLevel::Majority => "majority",
            DurabilityLevel::MajorityAndPersistOnMaster => "majorityAndPersistActive",
            DurabilityLevel::PersistToMajority => "persistToMajority",
            _ => "clientVerified",
        };

        write!(f, "{}", alias)
    }
}

impl TryFrom<&str> for DurabilityLevel {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "none" => Ok(DurabilityLevel::None),
            "majority" => Ok(DurabilityLevel::Majority),
            "majorityAndPersistActive" => Ok(DurabilityLevel::MajorityAndPersistOnMaster),
            "persistToMajority" => Ok(DurabilityLevel::PersistToMajority),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid or unsupported durability mode".into());
                Err(Generic { ctx })
            }
        }
    }
}
