use crate::{CouchbaseError, CouchbaseResult, ErrorContext};
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Receiver;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_json::Value;

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

    pub async fn meta_data(&mut self) -> CouchbaseResult<AnalyticsMetaData> {
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

#[derive(Debug, Deserialize)]
pub struct AnalyticsMetaData {
    #[serde(rename = "requestID")]
    request_id: String,
    #[serde(rename = "clientContextID")]
    client_context_id: String,
}
