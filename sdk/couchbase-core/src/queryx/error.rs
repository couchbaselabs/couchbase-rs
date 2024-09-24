use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use bytes::Bytes;
use http::StatusCode;
use log::error;
use serde::de::StdError;
use serde_json::Value;
use thiserror::Error;

use crate::httpx;
use crate::memdx::magic::Magic::Res;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    pub kind: Box<ErrorKind>,

    pub endpoint: String,
    pub statement: String,
    pub client_context_id: String,

    pub error_descs: Vec<ErrorDesc>,
    pub status_code: Option<StatusCode>,
}

impl Error {
    pub fn new_server_error(
        e: ServerError,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
        error_descs: Vec<ErrorDesc>,
        status_code: StatusCode,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::ServerError(e)),
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id: client_context_id.into(),
            error_descs,
            status_code: Some(status_code),
        }
    }

    pub fn new_http_error(
        e: httpx::error::Error,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::HttpError(e)),
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id: client_context_id.into(),
            error_descs: vec![],
            status_code: None,
        }
    }

    pub fn new_generic_error(
        msg: impl Into<String>,
        endpoint: impl Into<String>,
        statement: impl Into<String>,
        client_context_id: impl Into<String>,
    ) -> Error {
        Self {
            kind: Box::new(ErrorKind::Generic { msg: msg.into() }),
            endpoint: endpoint.into(),
            statement: statement.into(),
            client_context_id: client_context_id.into(),
            error_descs: vec![],
            status_code: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorDesc {
    pub kind: Box<ErrorKind>,

    pub code: u32,
    pub message: String,
    pub retry: bool,
    pub reason: HashMap<String, Value>,
}

impl ErrorDesc {
    pub fn new(
        kind: ErrorKind,
        code: u32,
        message: String,
        retry: bool,
        reason: HashMap<String, Value>,
    ) -> Self {
        Self {
            kind: Box::new(kind),
            code,
            message,
            retry,
            reason,
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{kind}")]
#[non_exhaustive]
pub struct ServerError {
    pub kind: Box<ServerErrorKind>,

    pub code: Option<u32>,
    pub msg: Option<String>,
}

impl ServerError {
    pub fn new(
        kind: ServerErrorKind,
        code: impl Into<Option<u32>>,
        msg: impl Into<Option<String>>,
    ) -> Self {
        Self {
            kind: Box::new(kind),
            code: code.into(),
            msg: msg.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceError {
    pub cause: ServerError,
    pub bucket_name: Option<String>,
    pub scope_name: Option<String>,
    pub collection_name: Option<String>,
    pub index_name: Option<String>,
}

impl ResourceError {
    pub fn new(cause: ServerError, msg: impl Into<String>) -> Self {
        match *cause.kind {
            ServerErrorKind::CollectionNotFound => {
                Self::parse_resource_not_found(cause, msg.into())
            }
            ServerErrorKind::ScopeNotFound => Self::parse_resource_not_found(cause, msg.into()),
            ServerErrorKind::AuthenticationFailure => Self::parse_auth_failure(cause, msg.into()),
            ServerErrorKind::IndexNotFound => {
                Self::parse_index_not_found_or_exists(cause, msg.into())
            }
            ServerErrorKind::IndexExists => {
                Self::parse_index_not_found_or_exists(cause, msg.into())
            }
            _ => Self {
                cause,
                bucket_name: None,
                scope_name: None,
                collection_name: None,
                index_name: None,
            },
        }
    }

    fn parse_index_not_found_or_exists(cause: ServerError, msg: String) -> ResourceError {
        let mut fields = msg.split_whitespace();

        // msg for not found is of the form - "Index Not Found - cause: GSI index testingIndex not found."
        // msg for index exists is of the form - "The index NewIndex already exists."
        while let Some(field) = fields.next() {
            if field == "index" {
                return ResourceError {
                    cause,
                    bucket_name: None,
                    scope_name: None,
                    collection_name: None,
                    index_name: Some(fields.next().unwrap().to_string()),
                };
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }

    fn parse_resource_not_found(cause: ServerError, msg: String) -> ResourceError {
        let mut fields = msg.split_whitespace();
        // Resource path is of the form bucket:bucket.scope.collection
        let path = fields.find(|f| f.contains('.') && f.contains(':'));

        if let Some(p) = path {
            if let Some(trimmed_path) = p.split(':').nth(1) {
                let fields: Vec<&str> = trimmed_path.split('.').collect();

                if *cause.kind == ServerErrorKind::ScopeNotFound {
                    // Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
                    let bucket_name = fields[0..fields.len() - 1].join(".");
                    let scope_name = fields[fields.len() - 1];

                    return ResourceError {
                        cause,
                        bucket_name: Some(bucket_name),
                        scope_name: Some(scope_name.to_string()),
                        collection_name: None,
                        index_name: None,
                    };
                } else if *cause.kind == ServerErrorKind::CollectionNotFound {
                    // Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
                    let bucket_name = fields[0..fields.len() - 2].join(".");
                    let scope_name = fields[fields.len() - 2];
                    let collection_name = fields[fields.len() - 1];

                    return ResourceError {
                        cause,
                        bucket_name: Some(bucket_name),
                        scope_name: Some(scope_name.to_string()),
                        collection_name: Some(collection_name.to_string()),
                        index_name: None,
                    };
                }
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }

    fn parse_auth_failure(cause: ServerError, msg: String) -> Self {
        let mut fields = msg.split_whitespace();
        let path = fields.find(|f| f.contains(':'));

        if let Some(p) = path {
            if let Some(trimmed_path) = p.split(':').nth(1) {
                let (bucket_name, scope_name, collection_name) = if trimmed_path.contains('`') {
                    // trimmedPath will have the form "`bucket.name`" or "`bucket.name`.scope.collection" so the first element of fields
                    // will be the empty string
                    let fields: Vec<&str> = trimmed_path.split('`').collect();

                    let bucket_name = fields[1];

                    let (scope_name, collection_name) = if fields[2].is_empty() {
                        (None, None)
                    } else {
                        // scope_and_col is of the form ".scope.collection" meaning fields[1] is empty and the names are in fields[1] and
                        // fields[2]
                        let scope_and_col = fields[2];
                        let fields: Vec<&str> = scope_and_col.split('.').collect();
                        let (scope, collection) = if fields.len() >= 3 {
                            (Some(fields[1].to_string()), Some(fields[2].to_string()))
                        } else {
                            (None, None)
                        };

                        (scope, collection)
                    };

                    (Some(bucket_name.to_string()), scope_name, collection_name)
                } else {
                    let fields: Vec<&str> = trimmed_path.split('.').collect();

                    let bucket_name = fields[0];

                    let (scope_name, collection_name) = if fields.len() >= 3 {
                        (Some(fields[1].to_string()), Some(fields[2].to_string()))
                    } else {
                        (None, None)
                    };

                    (Some(bucket_name.to_string()), scope_name, collection_name)
                };

                return ResourceError {
                    cause,
                    bucket_name,
                    scope_name,
                    collection_name,
                    index_name: None,
                };
            }
        }

        ResourceError {
            cause,
            bucket_name: None,
            scope_name: None,
            collection_name: None,
            index_name: None,
        }
    }
}

impl Display for ResourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let msg = format!(
            "resource error: {} (bucket: {}, scope: {}, collection: {}, index: {})",
            self.cause,
            self.bucket_name.clone().unwrap_or_default(),
            self.scope_name.clone().unwrap_or_default(),
            self.collection_name.clone().unwrap_or_default(),
            self.index_name.clone().unwrap_or_default()
        );

        write!(f, "{}", msg)
    }
}

impl StdError for ResourceError {}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("Server error {0:?}")]
    ServerError(ServerError),
    #[error("Http error sending request or receiving response {0:?}")]
    HttpError(httpx::error::Error),
    #[error("{0:?}")]
    Resource(ResourceError),
    #[error("{msg}")]
    Generic { msg: String },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerErrorKind {
    #[error("Parsing failure")]
    ParsingFailure,
    #[error("Internal server error")]
    Internal,
    #[error("Authentication failure")]
    AuthenticationFailure,
    #[error("Cas mismatch")]
    CasMismatch,
    #[error("Doc not found")]
    DocNotFound,
    #[error("Doc exists")]
    DocExists,
    #[error("Planning failure")]
    PlanningFailure,
    #[error("Index failure")]
    IndexFailure,
    #[error("Prepared statement failure")]
    PreparedStatementFailure,
    #[error("Data service returned an error during execution of DML statement")]
    DMLFailure,
    #[error("Server timeout")]
    Timeout,
    #[error("Index exists")]
    IndexExists,
    #[error("Index not found")]
    IndexNotFound,
    #[error("write statement used in a read-only query")]
    WriteInReadOnlyMode,
    #[error("Scope not found")]
    ScopeNotFound,
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Server invalid argument: (argument: {argument}, reason: {reason})")]
    InvalidArgument { argument: String, reason: String },
    #[error("Build already in progress")]
    BuildAlreadyInProgress,
    #[error("Unknown query error: {msg}")]
    #[non_exhaustive]
    Unknown { msg: String },
}
