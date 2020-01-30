use crate::api::error::CouchbaseResult;
use crate::api::options::*;
use crate::api::results::*;
use futures::channel::oneshot::Sender;
use std::time::Duration;

#[derive(Debug)]
pub enum Request {
    Get(GetRequest),
    Mutate(MutateRequest),
    Query(QueryRequest),
    Analytics(AnalyticsRequest),
    Exists(ExistsRequest),
    Remove(RemoveRequest),
}

#[derive(Debug)]
pub struct GetRequest {
    pub(crate) id: String,
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
    pub(crate) sender: Sender<CouchbaseResult<ExistsResult>>,
    pub(crate) options: ExistsOptions,
}

#[derive(Debug)]
pub struct RemoveRequest {
    pub(crate) id: String,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) options: RemoveOptions,
}

#[derive(Debug)]
pub struct MutateRequest {
    pub(crate) id: String,
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
