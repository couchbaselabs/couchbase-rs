use std::pin::{pin, Pin};
use std::ptr::read;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::err;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use log::debug;
use regex::Regex;
use tokio::sync::Mutex;

use crate::helpers::durations::parse_duration_from_golang_string;
use crate::httpx::json_row_stream::JsonRowStream;
use crate::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use crate::httpx::response::Response;
use crate::memdx::magic::Magic::Res;
use crate::queryx::error;
use crate::queryx::error::{
    Error, ErrorDesc, ErrorKind, ResourceError, ServerError, ServerErrorKind,
};
use crate::queryx::query_json::{
    QueryEarlyMetaData, QueryError, QueryErrorResponse, QueryMetaData, QueryMetrics, QueryWarning,
};
use crate::queryx::query_result::{EarlyMetaData, MetaData, Metrics, Warning};

pub struct QueryRespReader {
    endpoint: String,
    statement: String,
    client_context_id: String,
    status_code: StatusCode,

    streamer: Option<RawJsonRowStreamer>,
    early_meta_data: EarlyMetaData,
    meta_data: Option<MetaData>,
    meta_data_error: Option<Error>,
}

impl Stream for QueryRespReader {
    type Item = error::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut streamer: &mut RawJsonRowStreamer = if let Some(streamer) = this.streamer.as_mut() {
            streamer
        } else {
            return Poll::Ready(None);
        };

        match streamer.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row_data))) => Poll::Ready(Some(Ok(Bytes::from(row_data)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::new_http_error(
                &this.endpoint,
                this.statement.clone(),
                this.client_context_id.clone(),
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

impl QueryRespReader {
    pub async fn new(
        resp: Response,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
    ) -> error::Result<Self> {
        let status_code = resp.status();
        let endpoint = endpoint.into();
        let statement = statement.into();
        let client_context_id = client_context_id.into();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", &e);
                    return Err(
                        Error::new_http_error(endpoint, statement, client_context_id)
                            .with(Arc::new(e)),
                    );
                }
            };

            let errors: QueryErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(Error::new_message_error(
                        format!(
                        "non-200 status code received {} but parsing error response body failed {}",
                        status_code, e
                    ),
                        None,
                        None,
                        None,
                    ));
                }
            };

            if errors.errors.is_empty() {
                return Err(Error::new_message_error(
                    format!(
                        "Non-200 status code received {} but response body contained no errors",
                        status_code,
                    ),
                    None,
                    None,
                    None,
                ));
            }

            return Err(Self::parse_errors(
                &errors.errors,
                endpoint,
                statement,
                client_context_id,
                status_code,
            ));
        }

        let stream = resp.bytes_stream();
        let mut streamer = RawJsonRowStreamer::new(JsonRowStream::new(stream), "results");

        let early_meta_data =
            Self::read_early_metadata(&mut streamer, &endpoint, &statement, &client_context_id)
                .await?;

        let has_more_rows = streamer.has_more_rows();

        let mut reader = Self {
            endpoint,
            statement,
            client_context_id,
            status_code,
            streamer: Some(streamer),
            early_meta_data,
            meta_data: None,
            meta_data_error: None,
        };

        if !has_more_rows {
            reader.read_final_metadata();
            if let Some(e) = reader.meta_data_error {
                return Err(e);
            }
        }

        Ok(reader)
    }

    pub fn early_metadata(&self) -> &EarlyMetaData {
        &self.early_meta_data
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(e) = &self.meta_data_error {
            return Err(e.clone());
        }

        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(Error::new_message_error(
            "cannot read meta-data until after all rows are read",
            None,
            None,
            None,
        ))
    }

    async fn read_early_metadata(
        streamer: &mut RawJsonRowStreamer,
        endpoint: &str,
        statement: &str,
        client_context_id: &str,
    ) -> error::Result<EarlyMetaData> {
        let prelude = streamer.read_prelude().await.map_err(|e| {
            Error::new_http_error(
                endpoint,
                statement.to_string(),
                client_context_id.to_string(),
            )
            .with(Arc::new(e))
        })?;

        let early_metadata: QueryEarlyMetaData = serde_json::from_slice(&prelude).map_err(|e| {
            Error::new_message_error(
                format!("failed to parse metadata from response: {}", e),
                endpoint.to_string(),
                statement.to_string(),
                client_context_id.to_string(),
            )
        })?;

        Ok(EarlyMetaData {
            prepared: early_metadata.prepared,
        })
    }

    fn read_final_metadata(&mut self) {
        // We take the streamer here so that it gets dropped and closes the stream.
        let streamer = std::mem::take(&mut self.streamer);

        if let Some(mut streamer) = streamer {
            let epilog = match streamer.epilog() {
                Ok(e) => e,
                Err(e) => {
                    self.meta_data_error = Some(
                        Error::new_http_error(
                            &self.endpoint,
                            self.statement.clone(),
                            self.client_context_id.clone(),
                        )
                        .with(Arc::new(e)),
                    );
                    return;
                }
            };

            let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
                Ok(m) => m,
                Err(e) => {
                    self.meta_data_error = Some(Error::new_message_error(
                        format!("failed to parse metadata from response: {}", e),
                        self.endpoint.clone(),
                        self.statement.clone(),
                        self.client_context_id.clone(),
                    ));
                    return;
                }
            };

            let metadata = match self.parse_metadata(metadata) {
                Ok(m) => m,
                Err(e) => {
                    self.meta_data_error = Some(e);
                    return;
                }
            };

            self.meta_data = Some(metadata);
        }
    }

    fn parse_metadata(&self, metadata: QueryMetaData) -> error::Result<MetaData> {
        if !metadata.errors.is_empty() {
            return Err(Self::parse_errors(
                &metadata.errors,
                &self.endpoint,
                &self.statement,
                &self.client_context_id,
                self.status_code,
            ));
        }

        let metrics = self.parse_metrics(metadata.metrics);
        let warnings = self.parse_warnings(metadata.warnings);

        Ok(MetaData {
            prepared: metadata.early_meta_data.prepared,
            request_id: metadata.request_id.unwrap_or_default(),
            client_context_id: metadata.client_context_id.unwrap_or_default(),
            status: metadata.status,
            metrics,
            signature: metadata.signature,
            warnings,
            profile: metadata.profile,
        })
    }

    fn parse_metrics(&self, metrics: Option<QueryMetrics>) -> Metrics {
        if let Some(metrics) = metrics {
            let elapsed_time = if let Some(elapsed) = metrics.elapsed_time {
                parse_duration_from_golang_string(&elapsed).unwrap_or_default()
            } else {
                Duration::default()
            };

            let execution_time = if let Some(execution) = metrics.execution_time {
                parse_duration_from_golang_string(&execution).unwrap_or_default()
            } else {
                Duration::default()
            };

            return Metrics {
                elapsed_time,
                execution_time,
                result_count: metrics.result_count.unwrap_or_default(),
                result_size: metrics.result_size.unwrap_or_default(),
                mutation_count: metrics.mutation_count.unwrap_or_default(),
                sort_count: metrics.sort_count.unwrap_or_default(),
                error_count: metrics.error_count.unwrap_or_default(),
                warning_count: metrics.warning_count.unwrap_or_default(),
            };
        }

        Metrics::default()
    }

    fn parse_warnings(&self, warnings: Vec<QueryWarning>) -> Vec<Warning> {
        let mut converted = vec![];
        for w in warnings {
            converted.push(Warning {
                code: w.code.unwrap_or_default(),
                message: w.msg.unwrap_or_default(),
            });
        }

        converted
    }

    fn parse_errors(
        errors: &[QueryError],
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
        status_code: StatusCode,
    ) -> Error {
        let error_descs: Vec<ErrorDesc> = errors
            .iter()
            .map(|error| ErrorDesc {
                kind: Self::parse_error_kind(error),
                code: error.code,
                message: error.msg.clone(),
                retry: error.retry.unwrap_or_default(),
                reason: error.reason.clone(),
            })
            .collect();

        let chosen_desc = error_descs
            .iter()
            .find(|desc| !desc.retry)
            .unwrap_or(&error_descs[0]);

        let mut server_error = ServerError::new(
            chosen_desc.kind.clone(),
            endpoint,
            status_code,
            chosen_desc.code,
            chosen_desc.message.clone(),
        )
        .with_client_context_id(client_context_id)
        .with_statement(statement);

        if error_descs.len() > 1 {
            server_error = server_error.with_error_descs(error_descs);
        }

        match server_error.kind() {
            ServerErrorKind::ScopeNotFound => {
                Error::new_resource_error(ResourceError::new(server_error))
            }
            ServerErrorKind::CollectionNotFound => {
                Error::new_resource_error(ResourceError::new(server_error))
            }
            ServerErrorKind::IndexNotFound => {
                Error::new_resource_error(ResourceError::new(server_error))
            }
            ServerErrorKind::IndexExists => {
                Error::new_resource_error(ResourceError::new(server_error))
            }
            ServerErrorKind::AuthenticationFailure => {
                if server_error.code() == 13014 {
                    Error::new_resource_error(ResourceError::new(server_error))
                } else {
                    Error::new_server_error(server_error)
                }
            }
            _ => Error::new_server_error(server_error),
        }
    }

    fn parse_error_kind(error: &QueryError) -> ServerErrorKind {
        let err_code = error.code;
        let err_code_group = err_code / 1000;

        if err_code_group == 4 {
            if err_code == 4040
                || err_code == 4050
                || err_code == 4060
                || err_code == 4070
                || err_code == 4080
                || err_code == 4090
            {
                ServerErrorKind::PreparedStatementFailure
            } else if err_code == 4300 {
                ServerErrorKind::IndexExists
            } else {
                ServerErrorKind::PlanningFailure
            }
        } else if err_code_group == 5 {
            let msg = error.msg.to_lowercase();
            if msg.contains("not enough") && msg.contains("replica") {
                ServerErrorKind::InvalidArgument {
                    argument: "num_replicas".to_string(),
                    reason: "not enough indexer nodes to create index with replica count"
                        .to_string(),
                }
            } else if msg.contains("build already in progress") {
                ServerErrorKind::BuildAlreadyInProgress
            } else if Regex::new(".*?ndex .*? already exist.*")
                .unwrap()
                .is_match(&error.msg)
            {
                ServerErrorKind::IndexExists
            } else {
                ServerErrorKind::Internal
            }
        } else if err_code_group == 12 {
            if err_code == 12003 {
                ServerErrorKind::CollectionNotFound
            } else if err_code == 12004 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 12009 {
                if !error.reason.is_empty() {
                    if let Some(code) = error.reason.get("code") {
                        if code == 12033 {
                            ServerErrorKind::CasMismatch
                        } else if code == 17014 {
                            ServerErrorKind::DocNotFound
                        } else if code == 17012 {
                            ServerErrorKind::DocExists
                        } else {
                            ServerErrorKind::DMLFailure
                        }
                    } else {
                        ServerErrorKind::DMLFailure
                    }
                } else if error.msg.to_lowercase().contains("cas mismatch") {
                    ServerErrorKind::CasMismatch
                } else {
                    ServerErrorKind::DMLFailure
                }
            } else if err_code == 12016 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 12021 {
                ServerErrorKind::ScopeNotFound
            } else {
                ServerErrorKind::IndexFailure
            }
        } else if err_code_group == 14 {
            ServerErrorKind::IndexFailure
        } else if err_code_group == 10 {
            ServerErrorKind::AuthenticationFailure
        } else if err_code == 1000 {
            ServerErrorKind::WriteInReadOnlyMode
        } else if err_code == 1080 {
            ServerErrorKind::Timeout
        } else if err_code == 3000 {
            ServerErrorKind::ParsingFailure
        } else if err_code == 13014 {
            ServerErrorKind::AuthenticationFailure
        } else {
            ServerErrorKind::Unknown
        }
    }
}
