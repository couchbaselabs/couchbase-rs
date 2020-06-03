use crate::api::error::CouchbaseResult;
use crate::api::options::*;
use crate::api::results::*;
use crate::api::{LookupInSpec, MutateInSpec};
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
    GenericManagementRequest(GenericManagementRequest),
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
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct GetRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
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
pub struct ExistsRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) sender: Sender<CouchbaseResult<ExistsResult>>,
    pub(crate) options: ExistsOptions,
}

#[derive(Debug)]
pub struct RemoveRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) options: RemoveOptions,
}

#[derive(Debug)]
pub struct MutateRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) content: Vec<u8>,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) ty: MutateRequestType,
}

#[derive(Debug)]
pub enum MutateRequestType {
    Insert { options: InsertOptions },
    Upsert { options: UpsertOptions },
    Replace { options: ReplaceOptions },
}

#[derive(Debug)]
pub struct QueryRequest {
    pub(crate) statement: String,
    pub(crate) sender: Sender<CouchbaseResult<QueryResult>>,
    pub(crate) options: QueryOptions,
}

#[derive(Debug)]
pub struct AnalyticsRequest {
    pub(crate) statement: String,
    pub(crate) sender: Sender<CouchbaseResult<AnalyticsResult>>,
    pub(crate) options: AnalyticsOptions,
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
    pub(crate) sender: Sender<CouchbaseResult<LookupInResult>>,
    pub(crate) specs: Vec<LookupInSpec>,
    pub(crate) options: LookupInOptions,
}

#[derive(Debug)]
pub struct MutateInRequest {
    pub(crate) id: String,
    pub(crate) bucket: String,
    pub(crate) sender: Sender<CouchbaseResult<MutateInResult>>,
    pub(crate) specs: Vec<MutateInSpec>,
    pub(crate) options: MutateInOptions,
}

#[derive(Debug)]
pub struct GenericManagementRequest {
    pub(crate) path: String,
    pub(crate) method: String,
    pub(crate) payload: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) sender: Sender<CouchbaseResult<GenericManagementResult>>,
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
        }
    }

    pub fn content_type(&mut self, content_type: String) {
        self.content_type = Some(content_type)
    }

    pub fn timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout)
    }
}
