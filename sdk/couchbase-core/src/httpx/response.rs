use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use http::StatusCode;
use serde::de::DeserializeOwned;

use crate::httpx::error;
use crate::httpx::error::Error;

#[derive(Debug)]
pub struct Response {
    inner: reqwest::Response,
}

impl From<reqwest::Response> for Response {
    fn from(value: reqwest::Response) -> Self {
        Self { inner: value }
    }
}

impl Response {
    pub fn status(&self) -> StatusCode {
        self.inner.status()
    }

    pub async fn bytes(self) -> error::Result<Bytes> {
        self.inner.bytes().await.map_err(|e| {
            Error::new_message_error(format!("failed to read bytes from response: {}", e))
        })
    }

    pub fn bytes_stream(self) -> impl Stream<Item = error::Result<Bytes>> + Unpin {
        self.inner.bytes_stream().map_err(|e| {
            Error::new_message_error(format!("failed to read bytes stream from response: {}", e))
        })
    }

    pub async fn json<T: DeserializeOwned>(self) -> error::Result<T> {
        self.inner.json().await.map_err(|e| {
            Error::new_decoding_error(format!("failed to decode body into json: {}", e))
        })
    }

    pub fn url(&self) -> &str {
        self.inner.url().as_str()
    }
}
