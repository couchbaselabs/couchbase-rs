use std::fmt::Display;

use crate::httpx::error::Error as HttpError;
use crate::memdx::error::Error as MemdxError;
use crate::queryx::error::Error as QueryError;
use crate::service_type::ServiceType;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug, Clone)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    pub kind: Box<ErrorKind>,
    // #[source]
    // pub(crate) source: Option<Box<dyn StdError + 'static>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self {
            kind: Box::new(kind),
            // source: None,
        }
    }

    // pub(crate) fn with_source(kind: ErrorKind, source: impl std::error::Error + 'static) -> Self {
    //     Self {
    //         kind: Box::new(kind),
    //         source: Some(Box::new(source)),
    //     }
    // }

    pub fn is_memdx_error(&self) -> Option<&MemdxError> {
        match self.kind.as_ref() {
            ErrorKind::Memdx(e) => Some(e),
            _ => None,
        }
    }

    pub(crate) fn new_invalid_arguments_error(msg: &str) -> Self {
        Self::new(ErrorKind::InvalidArgument {
            msg: msg.to_string(),
        })
    }

    pub(crate) fn new_internal_error(msg: &str) -> Self {
        Self::new(ErrorKind::Internal {
            msg: msg.to_string(),
        })
    }
}

#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("Vbucket map outdated")]
    VbucketMapOutdated,
    #[error("An error occurred during serialization/deserialization {msg}")]
    #[non_exhaustive]
    JSONError { msg: String },
    #[error("Invalid argument {msg}")]
    #[non_exhaustive]
    InvalidArgument { msg: String },
    #[error("{0}")]
    Memdx(MemdxError),
    #[error("{0}")]
    Query(QueryError),
    #[error("{0}")]
    Http(HttpError),
    #[error("Endpoint not known {endpoint}")]
    #[non_exhaustive]
    EndpointNotKnown { endpoint: String },
    #[error("no endpoints available")]
    #[non_exhaustive]
    NoEndpointsAvailable,
    #[error("Shutdown")]
    Shutdown,
    #[error("No bucket selected")]
    NoBucket,
    #[error("Illegal State {msg}")]
    IllegalState { msg: String },
    #[error("Invalid vbucket map")]
    InvalidVbucketMap,
    #[error("Collection manifest outdated: our manifest uid: {manifest_uid}, server manifest uid: {server_manifest_uid}")]
    CollectionManifestOutdated {
        manifest_uid: u64,
        server_manifest_uid: u64,
    },
    #[error("{msg}")]
    #[non_exhaustive]
    Generic { msg: String },
    #[error("Service not available {service}")]
    #[non_exhaustive]
    ServiceNotAvailable { service: ServiceType },
    #[error("Internal error {msg}")]
    #[non_exhaustive]
    Internal { msg: String },
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        Self {
            kind: Box::new(err.into()),
        }
    }
}

impl From<MemdxError> for Error {
    fn from(value: MemdxError) -> Self {
        Self::new(ErrorKind::Memdx(value))
    }
}

impl From<QueryError> for Error {
    fn from(value: QueryError) -> Self {
        Self::new(ErrorKind::Query(value))
    }
}

impl From<HttpError> for Error {
    fn from(value: HttpError) -> Self {
        Self::new(ErrorKind::Http(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::new(ErrorKind::JSONError {
            msg: value.to_string(),
        })
    }
}
