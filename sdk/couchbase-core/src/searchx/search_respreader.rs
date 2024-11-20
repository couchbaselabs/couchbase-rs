use crate::httpx::json_row_stream::JsonRowStream;
use crate::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use crate::httpx::response::Response;
use crate::searchx::error::{ErrorKind, ServerErrorKind};
use crate::searchx::search::{decode_common_error, Search};
use crate::searchx::search_result::{FacetResult, MetaData, Metrics, ResultHit};
use crate::searchx::{error, search_json};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use http::StatusCode;
use log::debug;
use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct SearchRespReader {
    endpoint: String,
    status_code: StatusCode,

    streamer: Option<RawJsonRowStreamer>,
    meta_data: Option<MetaData>,
    facets: Option<HashMap<String, FacetResult>>,
}

impl Stream for SearchRespReader {
    type Item = error::Result<ResultHit>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut streamer: &mut RawJsonRowStreamer = if let Some(streamer) = this.streamer.as_mut() {
            streamer
        } else {
            return Poll::Ready(None);
        };

        match streamer.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row_data))) => {
                let row: search_json::Row =
                    match serde_json::from_slice(&row_data).map_err(|e| error::Error {
                        kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                        backtrace: Backtrace::capture(),
                        source: Some(Box::new(e)),
                        endpoint: this.endpoint.clone(),
                        status_code: Some(this.status_code),
                    }) {
                        Ok(row) => row,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };

                Poll::Ready(Some(Ok(ResultHit::from(row))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(error::Error {
                kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                backtrace: Backtrace::capture(),
                source: Some(Box::new(e)),
                endpoint: this.endpoint.clone(),
                status_code: Some(this.status_code),
            }))),
            Poll::Ready(None) => {
                let fut = this.read_final_metadata();
                match Box::pin(fut).poll_unpin(cx) {
                    Poll::Ready(Ok(_)) => Poll::Ready(None),
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl SearchRespReader {
    pub async fn new(resp: Response, endpoint: impl Into<String>) -> error::Result<Self> {
        let endpoint = endpoint.into();

        let status_code = resp.status();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", e);
                    return Err(error::Error::new_server_error_with_source(
                        ServerErrorKind::Unknown,
                        e.to_string(),
                        endpoint,
                        status_code,
                        Box::new(e),
                    ));
                }
            };

            let err: search_json::ErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(error::Error {
                        kind: Box::new(ErrorKind::Generic { msg:
                        format!(
                            "non-200 status code received {} but parsing error response body failed {}",
                            status_code,
                            e
                        )}),
                        backtrace: Backtrace::capture(),
                        source: Some(Box::new(e)),
                        endpoint,
                        status_code: Some(status_code),
                    });
                }
            };

            return Err(decode_common_error(status_code, &err.error, endpoint));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(JsonRowStream::new(stream), "hits");

        streamer
            .read_prelude()
            .await
            .map_err(|e| error::Error::new_http_error(e, endpoint.clone()))?;

        let mut reader = Self {
            endpoint,
            status_code,
            streamer: Some(streamer),
            meta_data: None,
            facets: None,
        };

        if let Some(streamer) = &reader.streamer {
            if !streamer.has_more_rows() {
                reader.read_final_metadata().await?;
            }
        }

        Ok(reader)
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(error::Error::new_generic_error(
            "cannot read meta-data until after all rows are read",
            self.endpoint.clone(),
        ))
    }

    pub fn facets(&self) -> error::Result<&HashMap<String, FacetResult>> {
        if let Some(facets) = &self.facets {
            return Ok(facets);
        }

        Err(error::Error::new_generic_error(
            "cannot read facets until after all rows are read",
            self.endpoint.clone(),
        ))
    }

    async fn read_final_metadata(&mut self) -> error::Result<()> {
        // We take the streamer here so that it gets dropped and closes the stream.
        let streamer = std::mem::take(&mut self.streamer);

        if let Some(mut streamer) = streamer {
            let epilog = streamer.read_epilog().await.map_err(|e| {
                error::Error::new_server_error_with_source(
                    ServerErrorKind::Unknown,
                    e.to_string(),
                    &self.endpoint,
                    self.status_code,
                    Box::new(e),
                )
            })?;

            let metadata_json: search_json::SearchMetaData = serde_json::from_slice(&epilog)
                .map_err(|e| error::Error {
                    kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                    backtrace: Backtrace::capture(),
                    source: Some(Box::new(e)),
                    endpoint: self.endpoint.clone(),
                    status_code: Some(self.status_code),
                })?;

            let metadata = MetaData {
                errors: metadata_json.status.errors,
                metrics: Metrics {
                    failed_partition_count: metadata_json.status.failed,
                    max_score: metadata_json.max_score,
                    successful_partition_count: metadata_json.status.successful,
                    took: Duration::from_nanos(metadata_json.took),
                    total_hits: metadata_json.total_hits,
                    total_partition_count: metadata_json.status.total,
                },
            };

            let mut facets: HashMap<String, FacetResult> = HashMap::new();
            for (facet_name, facet_data) in metadata_json.facets {
                facets.insert(facet_name, facet_data.try_into()?);
            }

            self.meta_data = Some(metadata);
            self.facets = Some(facets);
        }

        Ok(())
    }
}
