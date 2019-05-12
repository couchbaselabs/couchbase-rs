//! Results returned from operations.

use std::fmt;
use std::str;

use crate::error::CouchbaseError;
use futures::sync::{mpsc, oneshot};
use futures::Future;
use futures::Stream;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_derive::Deserialize;
use serde_json::from_slice;

pub struct GetResult {
    cas: u64,
    encoded: Vec<u8>,
    flags: u32,
}

impl GetResult {
    pub fn new(cas: u64, encoded: Vec<u8>, flags: u32) -> Self {
        Self {
            cas,
            encoded,
            flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content_as<'a, T>(&'a self) -> Result<T, CouchbaseError>
    where
        T: Deserialize<'a>,
    {
        match from_slice(&self.encoded.as_slice()) {
            Ok(v) => Ok(v),
            Err(_e) => Err(CouchbaseError::DecodingError),
        }
    }
}

impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GetResult {{ cas: 0x{:x}, flags: 0x{:x}, encoded: {} }}",
            self.cas,
            self.flags,
            str::from_utf8(&self.encoded).unwrap()
        )
    }
}

pub struct MutationResult {
    cas: u64,
}

impl MutationResult {
    pub fn new(cas: u64) -> Self {
        Self { cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl fmt::Debug for MutationResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MutationResult {{ cas: 0x{:x} }}", self.cas)
    }
}

pub struct ExistsResult {
    cas: u64,
}

impl ExistsResult {
    pub fn new(cas: u64) -> Self {
        Self { cas }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl fmt::Debug for ExistsResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExistsResult {{ cas: 0x{:x} }}", self.cas)
    }
}

#[derive(Debug)]
pub struct QueryResult {
    rows: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    meta: Option<oneshot::Receiver<Vec<u8>>>,
}

#[derive(Debug, Deserialize)]
pub struct QueryMeta {
    #[serde(rename = "requestID")]
    request_id: String,
    status: String,
    metrics: QueryMetrics,
}

#[derive(Debug, Deserialize)]
pub struct QueryMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "resultCount")]
    result_count: usize,
    #[serde(rename = "resultSize")]
    result_size: usize,
}

impl QueryResult {
    pub fn new(rows: mpsc::UnboundedReceiver<Vec<u8>>, meta: oneshot::Receiver<Vec<u8>>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows_as<T>(&mut self) -> impl Stream<Item = T, Error = CouchbaseError>
    where
        T: DeserializeOwned,
    {
        self.rows
            .take()
            .expect("Rows already consumed!")
            .map(|v| from_slice::<T>(v.as_slice()).expect("Could not convert type"))
            .map_err(|_| CouchbaseError::FutureError) // todo: something is wrong here, wants () ?
    }

    pub fn meta(&mut self) -> impl Future<Item = QueryMeta, Error = CouchbaseError> {
        self.meta
            .take()
            .expect("Meta already consumed!")
            .map(|v| from_slice::<QueryMeta>(v.as_slice()).expect("Could not convert type"))
            .map_err(|_| CouchbaseError::FutureError) // cancelled
    }
}

#[derive(Debug)]
pub struct AnalyticsResult {
    rows: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    meta: Option<oneshot::Receiver<Vec<u8>>>,
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsMeta {
    #[serde(rename = "requestID")]
    request_id: String,
    status: String,
    metrics: AnalyticsMetrics,
}

#[derive(Debug, Deserialize)]
pub struct AnalyticsMetrics {
    #[serde(rename = "elapsedTime")]
    elapsed_time: String,
    #[serde(rename = "executionTime")]
    execution_time: String,
    #[serde(rename = "resultCount")]
    result_count: usize,
    #[serde(rename = "resultSize")]
    result_size: usize,
}

impl AnalyticsResult {
    pub fn new(rows: mpsc::UnboundedReceiver<Vec<u8>>, meta: oneshot::Receiver<Vec<u8>>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows_as<T>(&mut self) -> impl Stream<Item = T, Error = CouchbaseError>
    where
        T: DeserializeOwned,
    {
        self.rows
            .take()
            .expect("Rows already consumed!")
            .map(|v| from_slice::<T>(v.as_slice()).expect("Could not convert type"))
            .map_err(|_| CouchbaseError::FutureError) // todo: something is wrong here, wants () ?
    }

    pub fn meta(&mut self) -> impl Future<Item = AnalyticsMeta, Error = CouchbaseError> {
        self.meta
            .take()
            .expect("Meta already consumed!")
            .map(|v| from_slice::<AnalyticsMeta>(v.as_slice()).expect("Could not convert type"))
            .map_err(|_| CouchbaseError::FutureError) // cancelled
    }
}

#[derive(Debug)]
pub struct LookupInResult {
    cas: u64,
    fields: Vec<LookupInField>,
}

impl LookupInResult {
    pub(crate) fn new(cas: u64, fields: Vec<LookupInField>) -> Self {
        LookupInResult { cas, fields }
    }
}

#[derive(Debug)]
pub struct LookupInField {
    status: CouchbaseError,
    value: Vec<u8>,
}

impl LookupInField {
    pub fn new(status: CouchbaseError, value: Vec<u8>) -> Self {
        LookupInField { status, value }
    }
}

#[derive(Debug)]
pub struct MutateInResult {
    cas: u64,
    fields: Vec<MutateInField>,
}

impl MutateInResult {
    pub(crate) fn new(cas: u64, fields: Vec<MutateInField>) -> Self {
        MutateInResult { cas, fields }
    }
}

#[derive(Debug)]
pub struct MutateInField {
    status: CouchbaseError,
    value: Vec<u8>,
}

impl MutateInField {
    pub fn new(status: CouchbaseError, value: Vec<u8>) -> Self {
        MutateInField { status, value }
    }
}
