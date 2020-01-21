use crate::api::error::CouchbaseResult;
use crate::api::options::{UpsertOptions, GetOptions, QueryOptions};
use crate::api::results::{MutationResult, GetResult, QueryResult};
use futures::channel::oneshot::Sender;

#[derive(Debug)]
pub enum Request {
    Get(GetRequest),
    Upsert(UpsertRequest),
    Query(QueryRequest),
}

#[derive(Debug)]
pub struct GetRequest {
    id: String,
    sender: Sender<CouchbaseResult<GetResult>>,
    options: GetOptions,
}

impl GetRequest {
    pub fn new(
        id: String,
        options: GetOptions,
        sender: Sender<CouchbaseResult<GetResult>>,
    ) -> Self {
        Self {
            id,
            options,
            sender,
        }
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn sender(self) -> Sender<CouchbaseResult<GetResult>> {
        self.sender
    }

    pub fn options(&self) -> &GetOptions {
        &self.options
    }
}

#[derive(Debug)]
pub struct UpsertRequest {
    id: String,
    content: Vec<u8>,
    sender: Sender<CouchbaseResult<MutationResult>>,
    options: UpsertOptions,
}

impl UpsertRequest {
    pub fn new(
        id: String,
        content: Vec<u8>,
        options: UpsertOptions,
        sender: Sender<CouchbaseResult<MutationResult>>,
    ) -> Self {
        Self {
            id,
            content,
            options,
            sender,
        }
    }

    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn sender(self) -> Sender<CouchbaseResult<MutationResult>> {
        self.sender
    }

    pub fn options(&self) -> &UpsertOptions {
        &self.options
    }

    pub fn content(&self) -> &[u8] {
        &self.content.as_ref()
    }
}

#[derive(Debug)]
pub struct QueryRequest {
    statement: String,
    sender: Sender<CouchbaseResult<QueryResult>>,
    options: QueryOptions,
}

impl QueryRequest {
    pub fn new(
        statement: String,
        options: QueryOptions,
        sender: Sender<CouchbaseResult<QueryResult>>,
    ) -> Self {
        Self {
            statement,
            options,
            sender,
        }
    }

    pub fn statement(&self) -> &String {
        &self.statement
    }

    pub fn sender(self) -> Sender<CouchbaseResult<QueryResult>> {
        self.sender
    }

    pub fn options(&self) -> &QueryOptions {
        &self.options
    }
}
