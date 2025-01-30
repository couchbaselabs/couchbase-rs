use crate::httpx::json_row_stream::JsonRowStream;
use crate::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use crate::httpx::response::Response;
use crate::searchx::error::{ErrorKind, ServerError, ServerErrorKind};
use crate::searchx::search::{decode_common_error, Search};
use crate::searchx::search_result::{FacetResult, MetaData, Metrics, ResultHit};
use crate::searchx::{error, search_json};
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use http::StatusCode;
use log::debug;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct SearchRespReader {
    endpoint: String,
    status_code: StatusCode,

    index_name: String,
    streamer: Option<RawJsonRowStreamer>,
    meta_data: Option<MetaData>,
    epilogue_error: Option<error::Error>,
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
                let row: search_json::Row = match serde_json::from_slice(&row_data).map_err(|e| {
                    error::Error::new_message_error(
                        format!("failed to parse row: {}", &e),
                        this.endpoint.clone(),
                    )
                }) {
                    Ok(row) => row,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };

                Poll::Ready(Some(Ok(ResultHit::from(row))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(error::Error::new_http_error(
                this.endpoint.clone(),
            )
            .with(Arc::new(e))))),
            Poll::Ready(None) => {
                this.read_final_metadata();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl SearchRespReader {
    pub async fn new(
        resp: Response,
        index_name: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> error::Result<Self> {
        let endpoint = endpoint.into();
        let index_name = index_name.into();

        let status_code = resp.status();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", e);
                    return Err(error::Error::new_http_error(endpoint).with(Arc::new(e)));
                }
            };

            let err: search_json::ErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(error::Error::new_message_error(
                        format!(
                        "non-200 status code received {} but parsing error response body failed {}",
                        status_code,
                        e
                    ),
                        endpoint,
                    ));
                }
            };

            return Err(decode_common_error(
                index_name,
                status_code,
                &err.error,
                endpoint,
            ));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(JsonRowStream::new(stream), "hits");

        match streamer.read_prelude().await {
            Ok(_) => {}
            Err(e) => {
                return Err(error::Error::new_http_error(endpoint).with(Arc::new(e)));
            }
        };

        let has_more_rows = streamer.has_more_rows();

        let mut reader = Self {
            endpoint,
            status_code,
            index_name,
            streamer: Some(streamer),
            meta_data: None,
            facets: None,
            epilogue_error: None,
        };

        if !has_more_rows {
            reader.read_final_metadata();
            if let Some(e) = reader.epilogue_error {
                return Err(e);
            }
        }

        Ok(reader)
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(e) = &self.epilogue_error {
            return Err(e.clone());
        }

        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(error::Error::new_message_error(
            "cannot read meta-data until after all rows are read",
            None,
        ))
    }

    pub fn facets(&self) -> error::Result<&HashMap<String, FacetResult>> {
        if let Some(e) = &self.epilogue_error {
            return Err(e.clone());
        }

        if let Some(facets) = &self.facets {
            return Ok(facets);
        }

        Err(error::Error::new_message_error(
            "cannot read facets until after all rows are read",
            None,
        ))
    }

    fn read_final_metadata(&mut self) {
        // We take the streamer here so that it gets dropped and closes the stream.
        let streamer = std::mem::take(&mut self.streamer);

        if let Some(mut streamer) = streamer {
            let epilog = match streamer.epilog() {
                Ok(e) => e,
                Err(e) => {
                    self.epilogue_error =
                        Some(error::Error::new_http_error(&self.endpoint).with(Arc::new(e)));
                    return;
                }
            };

            let metadata_json: search_json::SearchMetaData = match serde_json::from_slice(&epilog) {
                Ok(m) => m,
                Err(e) => {
                    self.epilogue_error = Some(error::Error::new_message_error(
                        format!("failed to parse metadata: {}", &e),
                        self.endpoint.clone(),
                    ));
                    return;
                }
            };

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
                facets.insert(
                    facet_name,
                    match facet_data.try_into() {
                        Ok(f) => f,
                        Err(e) => {
                            self.epilogue_error = Some(e);
                            return;
                        }
                    },
                );
            }

            self.meta_data = Some(metadata);
            self.facets = Some(facets);
        }
    }
}
