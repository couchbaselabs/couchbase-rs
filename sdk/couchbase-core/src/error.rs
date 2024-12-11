use crate::analyticsx::error::Error as AnalyticsError;
use crate::httpx::error::Error as HttpError;
use crate::memdx::error::Error as MemdxError;
use crate::mgmtx::error::Error as MgmtError;
use crate::queryx::error::Error as QueryError;
use crate::searchx::error::Error as SearchError;
use crate::service_type::ServiceType;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug, Clone)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    pub kind: Arc<ErrorKind>,
    // #[source]
    // pub(crate) source: Option<Box<dyn StdError + 'static>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self {
            kind: Arc::new(kind),
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
            ErrorKind::Memdx { source, .. } => Some(source),
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

    pub(crate) fn new_memdx_error(
        source: MemdxError,
        dispatched_to: Option<SocketAddr>,
        dispatched_from: Option<SocketAddr>,
    ) -> Self {
        Self::new(ErrorKind::Memdx {
            source,
            dispatched_to: dispatched_to.map(|x| x.to_string()),
            dispatched_from: dispatched_from.map(|x| x.to_string()),
        })
    }
}

#[derive(thiserror::Error, Debug)]
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
    #[error("{source} dispatched to: {dispatched_to:?}, dispatched from: {dispatched_from:?}")]
    Memdx {
        source: MemdxError,
        dispatched_to: Option<String>,
        dispatched_from: Option<String>,
    },
    #[error("{0}")]
    Query(QueryError),
    #[error("{0}")]
    Search(SearchError),
    #[error("{0}")]
    Analytics(AnalyticsError),
    #[error("{0}")]
    Http(HttpError),
    #[error("{0}")]
    Mgmt(MgmtError),
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
    #[error("feature not available {feature}, {msg}")]
    #[non_exhaustive]
    FeatureNotAvailable { feature: String, msg: String },
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
            kind: Arc::new(err.into()),
        }
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

impl From<SearchError> for Error {
    fn from(value: SearchError) -> Self {
        Self::new(ErrorKind::Search(value))
    }
}

impl From<AnalyticsError> for Error {
    fn from(value: AnalyticsError) -> Self {
        Self::new(ErrorKind::Analytics(value))
    }
}

impl From<MgmtError> for Error {
    fn from(value: MgmtError) -> Self {
        Self::new(ErrorKind::Mgmt(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::new(ErrorKind::JSONError {
            msg: value.to_string(),
        })
    }
}
