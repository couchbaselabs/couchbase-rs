/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

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
            Error::new_message_error(format!("failed to read bytes from response: {e}"))
        })
    }

    pub fn bytes_stream(self) -> impl Stream<Item = error::Result<Bytes>> + Unpin {
        self.inner.bytes_stream().map_err(|e| {
            Error::new_message_error(format!("failed to read bytes stream from response: {e}"))
        })
    }

    pub async fn json<T: DeserializeOwned>(self) -> error::Result<T> {
        self.inner
            .json()
            .await
            .map_err(|e| Error::new_decoding_error(format!("failed to decode body into json: {e}")))
    }

    pub fn url(&self) -> &str {
        self.inner.url().as_str()
    }
}
