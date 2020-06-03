use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::MutationToken;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

#[derive(Debug)]
pub struct QueryResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<QueryMetaData>>,
}

impl QueryResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<QueryMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
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
            Err(_e) => Duration::from_secs(0),
        }
    }

    pub fn execution_time(&self) -> Duration {
        match parse_duration::parse(&self.execution_time) {
            Ok(d) => d,
            Err(_e) => Duration::from_secs(0),
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

#[derive(Debug)]
pub struct AnalyticsResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<AnalyticsMetaData>>,
}

impl AnalyticsResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<AnalyticsMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows<T>(&mut self) -> impl Stream<Item = CouchbaseResult<T>>
    where
        T: DeserializeOwned,
    {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> AnalyticsMetaData {
        self.meta.take().unwrap().await.unwrap()
    }
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchMetaData {
    errors: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct SearchRow {
    index: String,
    id: String,
    score: f32,
}

impl SearchRow {
    pub fn index(&self) -> &str {
        &self.index
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn score(&self) -> f32 {
        self.score
    }
}

#[derive(Debug)]
pub struct SearchResult {
    rows: Option<UnboundedReceiver<Vec<u8>>>,
    meta: Option<Receiver<SearchMetaData>>,
}

impl SearchResult {
    pub fn new(rows: UnboundedReceiver<Vec<u8>>, meta: Receiver<SearchMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows(&mut self) -> impl Stream<Item = CouchbaseResult<SearchRow>> {
        self.rows.take().expect("Can not consume rows twice!").map(
            |v| match serde_json::from_slice(v.as_slice()) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CouchbaseError::DecodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                }),
            },
        )
    }

    pub async fn meta_data(&mut self) -> SearchMetaData {
        self.meta.take().unwrap().await.unwrap()
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

pub struct ExistsResult {
    cas: Option<u64>,
    exists: bool,
}

impl ExistsResult {
    pub fn new(exists: bool, cas: Option<u64>) -> Self {
        Self { exists, cas }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn cas(&self) -> &Option<u64> {
        &self.cas
    }
}

impl fmt::Debug for ExistsResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExistsResult {{ exists: {:?}, cas: {:?} }}",
            self.exists,
            self.cas.map(|c| format!("0x{:x}", c))
        )
    }
}

pub struct MutationResult {
    cas: u64,
    mutation_token: Option<MutationToken>,
}

impl MutationResult {
    pub fn new(cas: u64, mutation_token: Option<MutationToken>) -> Self {
        Self {
            cas,
            mutation_token,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }
}

impl fmt::Debug for MutationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MutationResult {{ cas: 0x{:x}, mutation_token: {:?} }}",
            self.cas, self.mutation_token
        )
    }
}

#[derive(Debug)]
pub(crate) struct SubDocField {
    pub status: u32,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct MutateInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl MutateInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

#[derive(Debug)]
pub struct LookupInResult {
    content: Vec<SubDocField>,
    cas: u64,
}

impl LookupInResult {
    pub(crate) fn new(content: Vec<SubDocField>, cas: u64) -> Self {
        Self { content, cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content<'a, T>(&'a self, index: usize) -> CouchbaseResult<T>
    where
        T: serde::Deserialize<'a>,
    {
        match serde_json::from_slice(
            &self
                .content
                .get(index)
                .expect("index not found")
                .value
                .as_slice(),
        ) {
            Ok(v) => Ok(v),
            Err(e) => Err(CouchbaseError::DecodingFailure {
                ctx: ErrorContext::default(),
                source: e.into(),
            }),
        }
    }

    pub fn exists(&self, index: usize) -> bool {
        self.content.get(index).expect("index not found").status == 0
    }
}

#[derive(Debug)]
pub struct GenericManagementResult {
    status: u16,
    payload: Option<Vec<u8>>,
}

impl GenericManagementResult {
    pub fn new(status: u16, payload: Option<Vec<u8>>) -> Self {
        Self { status, payload }
    }

    pub fn payload(&self) -> Option<&Vec<u8>> {
        self.payload.as_ref()
    }

    pub fn http_status(&self) -> u16 {
        self.status
    }
}

#[derive(Debug)]
pub struct KvStatsResult {
    stats: Option<UnboundedReceiver<KvStat>>,
}

impl KvStatsResult {
    pub fn new(stats: UnboundedReceiver<KvStat>) -> Self {
        Self { stats: Some(stats) }
    }

    pub fn stats(&mut self) -> impl Stream<Item = KvStat> {
        self.stats.take().expect("Can not consume stats twice!")
    }
}

#[derive(Debug)]
pub struct KvStat {
    server: String,
    value: String,
    key: String,
}

impl KvStat {
    pub fn new(server: String, key: String, value: String) -> Self {
        Self { server, key, value }
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}
