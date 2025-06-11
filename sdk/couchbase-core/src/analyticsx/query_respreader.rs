use crate::analyticsx::error;
use crate::analyticsx::error::{Error, ErrorDesc, ErrorKind, ServerError, ServerErrorKind};
use crate::analyticsx::query_result::MetaData;
use crate::analyticsx::response_json::{QueryError, QueryErrorResponse, QueryMetaData};
use crate::httpx;
use crate::httpx::decoder::Decoder;
use crate::httpx::raw_json_row_streamer::{RawJsonRowItem, RawJsonRowStreamer};
use crate::httpx::response::Response;
use bytes::Bytes;
use futures::future::err;
use futures::StreamExt;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use http::StatusCode;
use log::debug;
use serde::Deserialize;
use std::fmt::format;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Running,
    Success,
    Failed,
    Timeout,
    Fatal,
    Unknown,
}

pub struct QueryRespReader {
    endpoint: String,
    statement: String,
    client_context_id: Option<String>,
    status_code: StatusCode,

    streamer: Pin<Box<dyn Stream<Item = httpx::error::Result<RawJsonRowItem>> + Send>>,
    meta_data: Option<MetaData>,
    meta_data_error: Option<Error>,
}

impl Stream for QueryRespReader {
    type Item = error::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.streamer.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(RawJsonRowItem::Row(row_data)))) => {
                Poll::Ready(Some(Ok(Bytes::from(row_data))))
            }
            Poll::Ready(Some(Ok(RawJsonRowItem::Metadata(metadata)))) => {
                match this.read_final_metadata(metadata) {
                    Ok(meta) => this.meta_data = Some(meta),
                    Err(e) => {
                        this.meta_data_error = Some(e.clone());
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::new_http_error(
                &this.endpoint,
                &this.statement,
                this.client_context_id.clone(),
            )
            .with(Arc::new(e))))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl QueryRespReader {
    pub async fn new(
        resp: Response,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: Option<String>,
    ) -> error::Result<Self> {
        let endpoint = endpoint.into();
        let statement = statement.into();

        let status_code = resp.status();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("Failed to read response body on error {}", e);
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
                        endpoint,
                        statement,
                        client_context_id,
                    ));
                }
            };

            if errors.errors.is_empty() {
                return Err(Error::new_message_error(
                    format!(
                        "non-200 status code received {} but no error message in response body",
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
        let mut streamer = RawJsonRowStreamer::new(Decoder::new(stream), "results");

        match streamer.read_prelude().await {
            Ok(_) => {}
            Err(e) => {
                return Err(
                    Error::new_http_error(endpoint, statement, client_context_id).with(Arc::new(e)),
                );
            }
        };

        let has_more_rows = streamer.has_more_rows().await;
        let mut epilog = None;
        if !has_more_rows {
            epilog = match streamer.epilog() {
                Ok(epilog) => Some(epilog),
                Err(e) => {
                    return Err(
                        Error::new_http_error(endpoint, statement, client_context_id)
                            .with(Arc::new(e)),
                    );
                }
            };
        }

        let mut reader = Self {
            endpoint,
            statement,
            client_context_id,
            status_code,
            streamer: Box::pin(streamer.into_stream()),
            meta_data: None,
            meta_data_error: None,
        };

        if let Some(epilog) = epilog {
            let meta = reader.read_final_metadata(epilog)?;

            reader.meta_data = Some(meta);
        }

        Ok(reader)
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
            self.endpoint.clone(),
            self.statement.clone(),
            self.client_context_id.clone(),
        ))
    }

    fn read_final_metadata(&mut self, epilog: Vec<u8>) -> error::Result<MetaData> {
        let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
            Ok(m) => m,
            Err(e) => {
                return Err(Error::new_message_error(
                    format!("failed to parse query metadata from epilog: {}", e),
                    self.endpoint.clone(),
                    self.statement.clone(),
                    self.client_context_id.clone(),
                ));
            }
        };

        self.parse_metadata(metadata)
    }

    fn parse_metadata(&self, metadata: QueryMetaData) -> error::Result<MetaData> {
        if let Some(errors) = metadata.errors {
            return Err(Self::parse_errors(
                &errors,
                &self.endpoint,
                &self.statement,
                self.client_context_id.clone(),
                self.status_code,
            ));
        }

        Ok(metadata.into())
    }

    fn parse_errors(
        errors: &[QueryError],
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: Option<String>,
        status_code: StatusCode,
    ) -> Error {
        let error_descs: Vec<ErrorDesc> = errors
            .iter()
            .map(|error| {
                ErrorDesc::new(Self::parse_error_kind(error), error.code, error.msg.clone())
            })
            .collect();

        let chosen_desc = &error_descs[0];

        let mut server_error = ServerError::new(
            chosen_desc.kind().clone(),
            endpoint,
            status_code,
            chosen_desc.code(),
            chosen_desc.message(),
        )
        .with_statement(statement);

        if let Some(client_context_id) = client_context_id {
            server_error = server_error.with_client_context_id(client_context_id);
        }

        if error_descs.len() > 1 {
            server_error = server_error.with_error_descs(error_descs);
        }

        Error::new_server_error(server_error)
    }

    fn parse_error_kind(error: &QueryError) -> ServerErrorKind {
        let err_code = error.code;
        let err_code_group = err_code / 1000;

        if err_code_group == 20 {
            ServerErrorKind::AuthenticationFailure
        } else if err_code_group == 24 {
            if err_code == 24000 {
                ServerErrorKind::ParsingFailure
            } else if err_code == 24006 {
                ServerErrorKind::LinkNotFound
            } else if err_code == 24025 || err_code == 24044 || err_code == 24045 {
                ServerErrorKind::DatasetNotFound
            } else if err_code == 24034 {
                ServerErrorKind::DataverseNotFound
            } else if err_code == 24039 {
                ServerErrorKind::DataverseExists
            } else if err_code == 24040 {
                ServerErrorKind::DatasetExists
            } else if err_code == 24047 {
                ServerErrorKind::IndexNotFound
            } else if err_code == 24048 {
                ServerErrorKind::IndexExists
            } else {
                ServerErrorKind::CompilationFailure
            }
        } else if err_code_group == 25 {
            ServerErrorKind::CompilationFailure
        } else if err_code == 23000 || err_code == 23003 {
            ServerErrorKind::TemporaryFailure
        } else if err_code == 23007 {
            ServerErrorKind::JobQueueFull
        } else {
            ServerErrorKind::Unknown
        }
    }
}
