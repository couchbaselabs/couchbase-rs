use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use http::StatusCode;
use serde::de::DeserializeOwned;

use crate::httpx::error;
use crate::httpx::error::ErrorKind::Generic;

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
        Ok(self.inner.bytes().await?)
    }

    pub fn bytes_stream(self) -> impl Stream<Item = error::Result<Bytes>> + Unpin {
        self.inner
            .bytes_stream()
            .map_err(|e| Generic { msg: e.to_string() }.into())
    }

    pub async fn json<T: DeserializeOwned>(self) -> error::Result<T> {
        Ok(self
            .inner
            .json()
            .await
            .map_err(|e| Generic { msg: e.to_string() })?)
    }
}
