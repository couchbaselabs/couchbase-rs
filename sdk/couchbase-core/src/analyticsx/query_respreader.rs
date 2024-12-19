use crate::analyticsx::error;
use crate::analyticsx::error::{Error, ErrorDesc, ErrorKind, ServerErrorKind};
use crate::analyticsx::query_result::MetaData;
use crate::analyticsx::response_json::{QueryError, QueryErrorResponse, QueryMetaData};
use crate::httpx::json_row_stream::JsonRowStream;
use crate::httpx::raw_json_row_streamer::RawJsonRowStreamer;
use crate::httpx::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use http::StatusCode;
use serde::Deserialize;
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

    streamer: Option<RawJsonRowStreamer>,
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
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::new_generic_error(
                e.to_string(),
                &this.endpoint,
                &this.statement,
                this.client_context_id.clone(),
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
        client_context_id: Option<String>,
    ) -> error::Result<Self> {
        let status_code = resp.status();
        if status_code != 200 {
            let body = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    return Err(Error {
                        kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                        source: Some(Arc::new(e)),
                        endpoint: endpoint.into(),
                        status_code: Some(status_code),
                        statement: statement.into(),
                        client_context_id,
                    });
                }
            };

            let errors: QueryErrorResponse = match serde_json::from_slice(&body) {
                Ok(e) => e,
                Err(e) => {
                    return Err(Error {
                        kind: Box::new(ErrorKind::Generic { msg: format!(
                            "non-200 status code received {} but parsing error response body failed {}",
                            status_code,
                            e
                        ) }),
                        source: Some(Arc::new(e)),
                        endpoint: endpoint.into(),
                        status_code: Some(status_code),
                        statement: statement.into(),
                        client_context_id,
                    });
                }
            };

            if errors.errors.is_empty() {
                return Err(Error {
                    kind: Box::new(ErrorKind::Generic {
                        msg: format!(
                            "Non-200 status code received {} but response body contained no errors",
                            status_code
                        ),
                    }),
                    source: None,
                    endpoint: endpoint.into(),
                    status_code: Some(status_code),
                    statement: statement.into(),
                    client_context_id,
                });
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

        match streamer.read_prelude().await {
            Ok(_) => {}
            Err(e) => {
                return Err(Error::new_generic_error_with_source(
                    e.to_string(),
                    endpoint.into(),
                    statement.into(),
                    client_context_id,
                    Arc::new(e),
                ));
            }
        };

        let has_more_rows = streamer.has_more_rows();

        let mut reader = Self {
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id,
            status_code,
            streamer: Some(streamer),
            meta_data: None,
            meta_data_error: None,
        };

        if !has_more_rows {
            reader.read_final_metadata();
            if let Some(e) = &reader.meta_data_error {
                return Err(e.clone());
            }
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

        Err(error::Error {
            kind: Box::new(ErrorKind::Generic {
                msg: "cannot read meta-data until after all rows are read".to_string(),
            }),
            source: None,
            endpoint: self.endpoint.clone(),
            status_code: Some(self.status_code),
            statement: self.statement.clone(),
            client_context_id: self.client_context_id.clone(),
        })
    }

    fn read_final_metadata(&mut self) {
        // We take the streamer here so that it gets dropped and closes the stream.
        let streamer = std::mem::take(&mut self.streamer);

        if let Some(mut streamer) = streamer {
            let epilog = match streamer.epilog() {
                Ok(e) => e,
                Err(e) => {
                    self.meta_data_error = Some(error::Error {
                        kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                        source: Some(Arc::new(e)),
                        endpoint: self.endpoint.clone(),
                        status_code: Some(self.status_code),
                        statement: self.statement.clone(),
                        client_context_id: self.client_context_id.clone(),
                    });
                    return;
                }
            };

            let metadata: QueryMetaData = match serde_json::from_slice(&epilog) {
                Ok(m) => m,
                Err(e) => {
                    self.meta_data_error = Some(error::Error {
                        kind: Box::new(ErrorKind::Generic { msg: e.to_string() }),
                        source: Some(Arc::new(e)),
                        endpoint: self.endpoint.clone(),
                        status_code: Some(self.status_code),
                        statement: self.statement.clone(),
                        client_context_id: self.client_context_id.clone(),
                    });
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
    ) -> error::Error {
        let error_descs: Vec<ErrorDesc> = errors
            .iter()
            .map(|error| ErrorDesc {
                kind: Box::new(Self::parse_error_kind(error)),
                code: error.code,
                message: error.msg.clone(),
            })
            .collect();

        error::Error {
            kind: Box::new(ErrorKind::Server { error_descs }),
            source: None,
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id,
            status_code: Some(status_code),
        }
    }

    fn parse_error_kind(error: &QueryError) -> ServerErrorKind {
        if let Some(err_code) = error.code {
            let err_code_group = err_code / 1000;

            let kind = if err_code_group == 20 {
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
                ServerErrorKind::Unknown {
                    msg: format!("unknown error code {}", err_code),
                }
            };

            return kind;
        }

        ServerErrorKind::Unknown {
            msg: "no error code".to_string(),
        }
    }
}
