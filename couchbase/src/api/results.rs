use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::MutationToken;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use std::fmt;
use futures::{StreamExt, Stream};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::time::Duration;

#[derive(Debug)]
pub struct QueryResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<QueryMetaData>>,
}

impl QueryResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<QueryMetaData>) -> Self {
        Self { rows: Some(rows), meta: Some(meta) }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>> where
    T: DeserializeOwned {
        self.rows.take().expect("Can not consume rows twice!").map(|v| {
            match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            }
        })
    }

    pub async fn meta_data(&mut self) -> QueryMetaData {
        self.meta.take().unwrap().await.unwrap()
    }
}

// TODO: add status, signature, profile, warnings

#[derive(Debug, Deserialize)]
pub struct QueryMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
    metrics: QueryMetrics,
}

impl QueryMetaData {

    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    pub fn request_id(&self) -> &str {
        self.request_id.as_ref()
    }

    pub fn client_context_id(&self) -> &str {
        self.client_context_id.as_ref()
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "sortCount", default)]
    sort_count: usize,
    #[serde(rename = "resultCount")]
    result_count: usize,
    #[serde(rename = "resultSize")]
    result_size: usize,
    #[serde(rename = "mutationCount", default)]
    mutation_count: usize,
    #[serde(rename = "errorCount", default)]
    error_count: usize,
    #[serde(rename = "warningCount", default)]
    warning_count: usize,
}

impl QueryMetrics {

    pub fn elapsed_time(&self) -> Duration {
        match parse_duration::parse(&self.elapsed_time) {
            Ok(d) => d,
            Err(_e) => return Duration::from_secs(0)
        }
    }

    pub fn execution_time(&self) -> Duration {
        match parse_duration::parse(&self.execution_time) {
            Ok(d) => d,
            Err(_e) => return Duration::from_secs(0)
        }
    }

    pub fn sort_count(&self) -> usize {
        self.sort_count
    }

    pub fn result_count(&self) -> usize {
        self.result_count
    }

    pub fn result_size(&self) -> usize {
        self.result_size
    }

    pub fn mutation_count(&self) -> usize {
        self.mutation_count
    }

    pub fn error_count(&self) -> usize {
        self.error_count
    }

    pub fn warning_count(&self) -> usize {
        self.warning_count
    }
}

pub struct GetResult {
    content: Vec<u8>,
    cas: u64,
    flags: u32,
}

impl GetResult {
    pub fn new(content: Vec<u8>, cas: u64, flags: u32) -> Self {
        Self {
            content,
            cas,
            flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(&self.content.as_slice()) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }
}

impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = match std::str::from_utf8(&self.content) {
            Ok(c) => c,
            Err(_e) => "<Not Valid/Printable UTF-8>",
        };
        write!(
            f,
            "GetResult {{ cas: 0x{:x}, flags: 0x{:x}, content: {} }}",
            self.cas, self.flags, content
        )
    }
}

#[derive(Debug)]
pub struct MutationResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
}

impl MutationResult {
    pub fn new( cas: u64, mutation_token: Option<MutationToken>) -> Self {
        Self { cas, mutation_token }
    }
}