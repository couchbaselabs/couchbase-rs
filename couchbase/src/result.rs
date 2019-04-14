use std::fmt;
use std::str;

use futures::sync::{mpsc, oneshot};
use futures::Future;
use futures::Stream;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::from_slice;

pub struct GetResult {
    cas: u64,
    encoded: Vec<u8>,
    flags: u32,
}

impl GetResult {
    pub fn new(cas: u64, encoded: Vec<u8>, flags: u32) -> Self {
        GetResult {
            cas,
            encoded,
            flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn content_as<'a, T>(&'a self) -> T
    where
        T: Deserialize<'a>,
    {
        from_slice(&self.encoded.as_slice()).expect("Could not convert type")
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
        MutationResult { cas }
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
        QueryResult {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows_as<T>(&mut self) -> impl Iterator<Item = T>
    where
        T: DeserializeOwned,
    {
        self.rows
            .take()
            .expect("Rows already consumed!")
            .map(|v| from_slice::<T>(v.as_slice()).expect("Could not convert type"))
            .wait()
            .map(|v| v.expect("could not unwrap row"))
    }

    pub fn meta(&mut self) -> QueryMeta {
        self.meta
            .take()
            .expect("Meta already consumed!")
            .map(|v| from_slice::<QueryMeta>(v.as_slice()).expect("Could not convert type"))
            .wait()
            .expect("could not unwrap meta")
    }
}
