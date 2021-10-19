use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct ViewMetaData {
    total_rows: u64,
    debug: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ViewRow {
    pub(crate) id: Option<String>,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl ViewRow {
    pub fn id(&self) -> Option<&String> {
        self.id.as_ref()
    }

    pub fn key<T>(&self) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(self.key.as_slice()).map_err(|e| CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })
    }

    pub fn value<T>(&self) -> CouchbaseResult<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(self.value.as_slice()).map_err(|e| CouchbaseError::DecodingFailure {
            ctx: ErrorContext::default(),
            source: e.into(),
        })
    }
}

#[derive(Debug)]
pub struct ViewResult {
    rows: Option<UnboundedReceiver<ViewRow>>,
    meta: Option<Receiver<ViewMetaData>>,
}

impl ViewResult {
    pub fn new(rows: UnboundedReceiver<ViewRow>, meta: Receiver<ViewMetaData>) -> Self {
        Self {
            rows: Some(rows),
            meta: Some(meta),
        }
    }

    pub fn rows(&mut self) -> impl Stream<Item = CouchbaseResult<ViewRow>> {
        self.rows
            .take()
            .expect("Can not consume rows twice!")
            .map(Ok)
        // .map(
        // |v| match serde_json::from_slice(v.as_slice()) {
        //     Ok(decoded) => Ok(decoded),
        //     Err(e) => Err(CouchbaseError::DecodingFailure {
        //         ctx: ErrorContext::default(),
        //         source: e.into(),
        //     }),
        // },
        // )
    }

    pub async fn meta_data(&mut self) -> CouchbaseResult<ViewMetaData> {
        self.meta
            .take()
            .expect("Can not consume metadata twice!")
            .await
            .map_err(|e| {
                let mut ctx = ErrorContext::default();
                ctx.insert("error", Value::String(e.to_string()));
                CouchbaseError::RequestCanceled { ctx }
            })
    }
}
