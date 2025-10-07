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
use crate::searchx::search_respreader::SearchRespReader;
use crate::searchx::search_result::{FacetResult, MetaData, ResultHit};
use futures::StreamExt;
use futures_core::Stream;
use std::collections::HashMap;

pub struct SearchResultStream {
    pub(crate) inner: SearchRespReader,
    pub(crate) endpoint: String,
}

impl SearchResultStream {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn facets(&self) -> error::Result<&HashMap<String, FacetResult>> {
        self.inner.facets().map_err(|e| e.into())
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        self.inner.metadata().map_err(|e| e.into())
    }
}

impl Stream for SearchResultStream {
    type Item = error::Result<ResultHit>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map_err(|e| e.into())
    }
}
