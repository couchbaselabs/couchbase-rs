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
