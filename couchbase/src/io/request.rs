use crate::api::error::CouchbaseResult;
use crate::api::options::{GetOptions, QueryOptions, UpsertOptions};
use crate::api::results::{GetResult, MutationResult, QueryResult};
use futures::channel::oneshot::Sender;

#[derive(Debug)]
pub enum Request {
    Get(GetRequest),
    Upsert(UpsertRequest),
    Query(QueryRequest),
}

#[derive(Debug)]
pub struct GetRequest {
    pub(crate) id: String,
    pub(crate) sender: Sender<CouchbaseResult<GetResult>>,
    pub(crate) options: GetOptions,
}

#[derive(Debug)]
pub struct UpsertRequest {
    pub(crate) id: String,
    pub(crate) content: Vec<u8>,
    pub(crate) sender: Sender<CouchbaseResult<MutationResult>>,
    pub(crate) options: UpsertOptions,
}

#[derive(Debug)]
pub struct QueryRequest {
    pub(crate) statement: String,
    pub(crate) sender: Sender<CouchbaseResult<QueryResult>>,
    pub(crate) options: QueryOptions,
}
