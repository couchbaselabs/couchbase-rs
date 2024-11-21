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
use crate::httpx::error::ErrorKind::Generic;
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
    early_meta_data: Option<EarlyMetaData>,
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
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::new_server_error(
                ServerError::new(ServerErrorKind::Unknown { msg: e.to_string() }, None, None),
                &this.endpoint,
                &this.statement,
                &this.client_context_id,
                vec![],
                this.status_code,
            )))),
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
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", e);
                    return Err(Error::new_server_error(
                        ServerError::new(
                            ServerErrorKind::Unknown { msg: e.to_string() },
                            None,
                            None,
                        ),
                        endpoint,
                        statement,
                        client_context_id,
                        vec![],
                        status_code,
                    ));
                }
            };

            let errors: QueryErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(Error::new_generic_error(
                            format!(
                                "non-200 status code received {} but parsing error response body failed {}",
                                status_code,
                                e
                            ),
                            endpoint,
                            statement,
                            client_context_id,
                        ));
                }
            };

            if errors.errors.is_empty() {
                return Err(Error::new_generic_error(
                    format!(
                        "Non-200 status code received {} but response body contained no errors",
                        status_code
                    ),
                    endpoint,
                    statement,
                    client_context_id,
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
        let streamer = RawJsonRowStreamer::new(JsonRowStream::new(stream), "results");

        let mut reader = Self {
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id: client_context_id.into(),
            status_code,
            streamer: Some(streamer),
            early_meta_data: None,
            meta_data: None,
            meta_data_error: None,
        };

        reader.read_early_metadata().await?;

        // TODO: this is a bit absurd.
        if let Some(streamer) = &reader.streamer {
            if !streamer.has_more_rows() {
                reader.read_final_metadata();
                if let Some(e) = &reader.meta_data_error {
                    return Err(e.clone());
                }
            }
        }

        Ok(reader)
    }

    pub fn early_metadata(&self) -> Option<&EarlyMetaData> {
        self.early_meta_data.as_ref()
    }

    pub fn metadata(&self) -> error::Result<&MetaData> {
        if let Some(e) = &self.meta_data_error {
            return Err(e.clone());
        }

        if let Some(meta) = &self.meta_data {
            return Ok(meta);
        }

        Err(Error::new_generic_error(
            "cannot read meta-data until after all rows are read",
            &self.endpoint,
            &self.statement,
            &self.client_context_id,
        ))
    }

    async fn read_early_metadata(&mut self) -> error::Result<()> {
        if let Some(ref mut streamer) = self.streamer {
            let prelude = streamer.read_prelude().await.map_err(|e| {
                Error::new_server_error(
                    ServerError::new(ServerErrorKind::Unknown { msg: e.to_string() }, None, None),
                    &self.endpoint,
                    &self.statement,
                    &self.client_context_id,
                    vec![],
                    self.status_code,
                )
            })?;

            let early_metadata: QueryEarlyMetaData =
                serde_json::from_slice(&prelude).map_err(|e| {
                    Error::new_generic_error(
                        e.to_string(),
                        &self.endpoint,
                        &self.statement,
                        &self.client_context_id,
                    )
                })?;

            self.early_meta_data = Some(EarlyMetaData {
                prepared: early_metadata.prepared,
            });

            return Ok(());
        }

        Err(Error::new_generic_error(
            "streamer already consumed",
            &self.endpoint,
            &self.statement,
            &self.client_context_id,
        ))
    }

    fn read_final_metadata(&mut self) {
        // We take the streamer here so that it gets dropped and closes the stream.
        let streamer = std::mem::take(&mut self.streamer);

        if let Some(mut streamer) = streamer {
            let epilog = match streamer.epilog() {
                Ok(e) => e,
                Err(e) => {
                    self.meta_data_error = Some(Error::new_server_error(
                        ServerError::new(
                            ServerErrorKind::Unknown { msg: e.to_string() },
                            None,
                            None,
                        ),
                        &self.endpoint,
                        &self.statement,
                        &self.client_context_id,
                        vec![],
                        self.status_code,
                    ));
                    return;
                }
            };

            let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
                Ok(m) => m,
                Err(e) => {
                    self.meta_data_error = Some(Error::new_generic_error(
                        e.to_string(),
                        &self.endpoint,
                        &self.statement,
                        &self.client_context_id,
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
                kind: Box::new(Self::parse_error_kind(error)),
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

        Error {
            kind: chosen_desc.kind.clone(),
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id: client_context_id.into(),
            error_descs,
            status_code: Some(status_code),
        }
    }

    fn parse_error_kind(error: &QueryError) -> ErrorKind {
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
                return ErrorKind::ServerError(ServerError::new(
                    ServerErrorKind::PreparedStatementFailure,
                    error.code,
                    error.msg.clone(),
                ));
            } else if err_code == 4300 {
                return ErrorKind::Resource(ResourceError::new(
                    ServerError::new(ServerErrorKind::IndexExists, error.code, error.msg.clone()),
                    &error.msg,
                ));
            }

            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::PlanningFailure,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code_group == 5 {
            let msg = error.msg.to_lowercase();
            if msg.contains("not enough") && msg.contains("replica") {
                return ErrorKind::ServerError(ServerError::new(
                    ServerErrorKind::InvalidArgument {
                        argument: "num_replicas".to_string(),
                        reason: "not enough indexer nodes to create index with replica count"
                            .to_string(),
                    },
                    error.code,
                    error.msg.clone(),
                ));
            } else if msg.contains("build already in progress") {
                return ErrorKind::ServerError(ServerError::new(
                    ServerErrorKind::BuildAlreadyInProgress,
                    error.code,
                    error.msg.clone(),
                ));
            } else if Regex::new(".*?ndex .*? already exist.*")
                .unwrap()
                .is_match(&error.msg)
            {
                return ErrorKind::ServerError(ServerError::new(
                    ServerErrorKind::IndexExists,
                    error.code,
                    error.msg.clone(),
                ));
            }

            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::Internal,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code_group == 12 {
            if err_code == 12003 {
                return ErrorKind::Resource(ResourceError::new(
                    ServerError::new(
                        ServerErrorKind::CollectionNotFound,
                        error.code,
                        error.msg.clone(),
                    ),
                    &error.msg,
                ));
            } else if err_code == 12004 {
                return ErrorKind::Resource(ResourceError::new(
                    ServerError::new(
                        ServerErrorKind::IndexNotFound,
                        error.code,
                        error.msg.clone(),
                    ),
                    &error.msg,
                ));
            } else if err_code == 12009 {
                if !error.reason.is_empty() {
                    if let Some(code) = error.reason.get("code") {
                        // if let Some(c) = code.as_f64() {
                        if code == 12033 {
                            return ErrorKind::ServerError(ServerError::new(
                                ServerErrorKind::CasMismatch,
                                error.code,
                                error.msg.clone(),
                            ));
                        } else if code == 17014 {
                            return ErrorKind::ServerError(ServerError::new(
                                ServerErrorKind::DocNotFound,
                                error.code,
                                error.msg.clone(),
                            ));
                        } else if code == 17012 {
                            return ErrorKind::ServerError(ServerError::new(
                                ServerErrorKind::DocExists,
                                error.code,
                                error.msg.clone(),
                            ));
                        };
                        // }
                    }
                } else if error.msg.to_lowercase().contains("cas mismatch") {
                    return ErrorKind::ServerError(ServerError::new(
                        ServerErrorKind::CasMismatch,
                        error.code,
                        error.msg.clone(),
                    ));
                }

                return ErrorKind::ServerError(ServerError::new(
                    ServerErrorKind::DMLFailure,
                    error.code,
                    error.msg.clone(),
                ));
            } else if err_code == 12016 {
                return ErrorKind::Resource(ResourceError::new(
                    ServerError::new(
                        ServerErrorKind::IndexNotFound,
                        error.code,
                        error.msg.clone(),
                    ),
                    &error.msg,
                ));
            } else if err_code == 12021 {
                return ErrorKind::Resource(ResourceError::new(
                    ServerError::new(
                        ServerErrorKind::ScopeNotFound,
                        error.code,
                        error.msg.clone(),
                    ),
                    &error.msg,
                ));
            }

            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::IndexFailure,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code_group == 14 {
            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::IndexFailure,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code_group == 10 {
            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::AuthenticationFailure,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code == 1000 {
            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::WriteInReadOnlyMode,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code == 1080 {
            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::Timeout,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code == 3000 {
            return ErrorKind::ServerError(ServerError::new(
                ServerErrorKind::ParsingFailure,
                error.code,
                error.msg.clone(),
            ));
        } else if err_code == 13014 {
            return ErrorKind::Resource(ResourceError::new(
                ServerError::new(
                    ServerErrorKind::AuthenticationFailure,
                    error.code,
                    error.msg.clone(),
                ),
                &error.msg,
            ));
        }

        ErrorKind::ServerError(ServerError::new(
            ServerErrorKind::Unknown {
                msg: "unknown query error".to_string(),
            },
            error.code,
            error.msg.clone(),
        ))
    }
}
