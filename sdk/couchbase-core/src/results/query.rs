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

use crate::error;
use crate::queryx::query_respreader::QueryRespReader;
use crate::queryx::query_result::{EarlyMetaData, MetaData};
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;

pub struct QueryResultStream {
    pub(crate) inner: QueryRespReader,
    pub(crate) endpoint: String,
}

impl QueryResultStream {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn early_metadata(&self) -> &EarlyMetaData {
        self.inner.early_metadata()
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        self.inner.metadata().map_err(|e| e.into())
    }
}

impl Stream for QueryResultStream {
    type Item = error::Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map_err(|e| e.into())
    }
}
