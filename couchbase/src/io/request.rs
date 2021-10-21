use crate::api::error::{CouchbaseError, CouchbaseResult};
use crate::api::keyvalue_options::*;
use crate::api::keyvalue_results::*;
use crate::api::results::*;
use crate::{
    AnalyticsOptions, AnalyticsResult, LookupInOptions, LookupInResult, LookupInSpec,
    MutateInOptions, MutateInResult, MutateInSpec, QueryOptions, QueryResult, SearchOptions,
    SearchResult, ServiceType, ViewResult,
};
use futures::channel::oneshot::Sender;
use std::time::Duration;

#[derive(Debug)]
pub enum Request {
    Get(GetRequest),
    Mutate(MutateRequest),
    Exists(ExistsRequest),
    Remove(RemoveRequest),
    MutateIn(MutateInRequest),
    LookupIn(LookupInRequest),
    Query(QueryRequest),
    Analytics(AnalyticsRequest),
    Search(SearchRequest),
    View(ViewRequest),
    GenericManagement(GenericManagementRequest),
    Ping(PingRequest),
    Counter(CounterRequest),
    Unlock(UnlockRequest),
    Touch(TouchRequest),
    GetReplica(GetReplicaRequest),
}

impl Request {
    pub fn bucket(&self) -> Option<&String> {
        match self {
            Self::Get(r) => Some(&r.bucket),
            Self::Mutate(r) => Some(&r.bucket),
            Self::Exists(r) => Some(&r.bucket),
            Self::Remove(r) => Some(&r.bucket),
            Self::MutateIn(r) => Some(&r.bucket),
            Self::LookupIn(r) => Some(&r.bucket),
            Self::Counter(r) => Some(&r.bucket),
            Self::Unlock(r) => Some(&r.bucket),
            Self::Touch(r) => Some(&r.bucket),
            Self::GetReplica(r) => Some(&r.bucket),
            _ => None,
        }
    }

    pub fn fail(self, reason: CouchbaseError) {
        match self {
            Self::Get(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Mutate(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Exists(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Remove(r) => r.sender.send(Err(reason)).unwrap(),
            Self::MutateIn(r) => r.sender.send(Err(reason)).unwrap(),
            Self::LookupIn(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Query(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Analytics(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Search(r) => r.sender.send(Err(reason)).unwrap(),
            Self::View(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Ping(r) => r.sender.send(Err(reason)).unwrap(),
            Self::GenericManagement(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Counter(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Unlock(r) => r.sender.send(Err(reason)).unwrap(),
            Self::Touch(r) => r.sender.send(Err(reason)).unwrap(),
            Self::GetReplica(r) => r.sender.send(Err(reason)).unwrap(),
        };
    }
}

#[derive(Debug)]
pub struct GetRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<GetResult>>,
    pub(crate) ty: GetRequestType,
}

#[derive(Debug)]
pub enum GetRequestType {
    Get {
        options: GetOptions,
    },
    GetAndLock {
        options: GetAndLockOptions,
        lock_time: Duration,
    },
    GetAndTouch {
        options: GetAndTouchOptions,
        expiry: Duration,
    },
}

#[derive(Debug)]
pub struct GetReplicaRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<GetReplicaResult>>,
    pub(crate) options: GetReplicaOptions,
    pub(crate) mode: ReplicaMode,
}

#[derive(Debug)]
pub struct ExistsRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<ExistsResult>>,
    pub(crate) options: ExistsOptions,
}

#[derive(Debug)]
pub struct RemoveRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) options: RemoveOptions,
}

#[derive(Debug)]
pub struct TouchRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) expiry: Duration,
    pub(crate) options: TouchOptions,
}

#[derive(Debug)]
pub struct UnlockRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<()>>,
    pub(crate) cas: u64,
    pub(crate) options: UnlockOptions,
}

#[derive(Debug)]
pub struct MutateRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) content: Vec<u8>,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) ty: MutateRequestType,
}

#[derive(Debug)]
pub enum MutateRequestType {
    Insert { options: InsertOptions },
    Upsert { options: UpsertOptions },
    Replace { options: ReplaceOptions },
    Append { options: AppendOptions },
    Prepend { options: PrependOptions },
}

#[derive(Debug)]
pub struct CounterRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<CounterResult>>,
    pub(crate) options: CounterOptions,
}

#[derive(Debug)]
pub struct QueryRequest {
    pub(crate) statement: String,
    pub(crate) sender: Sender<CouchbaseResult<QueryResult>>,
    pub(crate) options: QueryOptions,
    pub(crate) scope: Option<String>,
}

#[derive(Debug)]
pub struct AnalyticsRequest {
    pub(crate) statement: String,
    pub(crate) sender: Sender<CouchbaseResult<AnalyticsResult>>,
    pub(crate) options: AnalyticsOptions,
    pub(crate) scope: Option<String>,
}

#[derive(Debug)]
pub struct SearchRequest {
    pub(crate) index: String,
    pub(crate) query: serde_json::Value,
    pub(crate) sender: Sender<CouchbaseResult<SearchResult>>,
    pub(crate) options: SearchOptions,
}

#[derive(Debug)]
pub struct LookupInRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<LookupInResult>>,
    pub(crate) specs: Vec<LookupInSpec>,
    pub(crate) options: LookupInOptions,
}

#[derive(Debug)]
pub struct MutateInRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) scope: String,
    pub(crate) collection: String,
    pub(crate) sender: Sender<CouchbaseResult<MutateInResult>>,
    pub(crate) specs: Vec<MutateInSpec>,
    pub(crate) options: MutateInOptions,
}

#[derive(Debug)]
pub struct ViewRequest {
    pub(crate) design_document: String,
    pub(crate) view_name: String,
    pub(crate) sender: Sender<CouchbaseResult<ViewResult>>,
    pub(crate) options: Vec<u8>,
}

#[derive(Debug)]
pub struct GenericManagementRequest {
    pub(crate) path: String,
    pub(crate) method: String,
    pub(crate) payload: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) sender: Sender<CouchbaseResult<GenericManagementResult>>,
    pub(crate) service_type: Option<ServiceType>,
}

impl GenericManagementRequest {
    pub fn new(
        sender: Sender<CouchbaseResult<GenericManagementResult>>,
        path: String,
        method: String,
        payload: Option<String>,
    ) -> Self {
        Self {
            sender,
            path,
            method,
            payload,
            content_type: None,
            timeout: None,
            service_type: None,
        }
    }

    pub fn content_type(&mut self, content_type: String) {
        self.content_type = Some(content_type)
    }

    pub fn timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout)
    }

    pub fn service_type(&mut self, service_type: ServiceType) {
        self.service_type = Some(service_type)
    }
}

#[derive(Debug)]
pub struct PingRequest {
    pub(crate) sender: Sender<CouchbaseResult<PingResult>>,
    pub(crate) options: PingOptions,
}
