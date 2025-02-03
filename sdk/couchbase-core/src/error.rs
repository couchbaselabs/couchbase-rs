use crate::analyticsx::error::Error as AnalyticsError;
use crate::httpx::error::Error as HttpError;
use crate::memdx;
use crate::mgmtx::error::Error as MgmtError;
use crate::queryx::error::Error as QueryError;
use crate::searchx::error::Error as SearchError;
use crate::service_type::ServiceType;
use serde::de::StdError;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Error {
    kind: Arc<ErrorKind>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self.kind.as_ref() {
            ErrorKind::Memdx(err) => err.source.source(),
            ErrorKind::Query(err) => err.source(),
            ErrorKind::Search(err) => err.source(),
            ErrorKind::Analytics(err) => err.source(),
            ErrorKind::Http(err) => err.source(),
            ErrorKind::Mgmt(err) => err.source(),
            _ => None,
        }
    }
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self {
            kind: Arc::new(kind),
        }
    }

    pub(crate) fn new_contextual_memdx_error(e: MemdxError) -> Self {
        Self::new(ErrorKind::Memdx(e))
    }

    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Message { msg: msg.into() })
    }

    pub(crate) fn new_invalid_argument_error(msg: impl Into<String>, arg: Option<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument {
            msg: msg.into(),
            arg,
        })
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub(crate) fn is_memdx_error(&self) -> Option<&memdx::error::Error> {
        match self.kind.as_ref() {
            ErrorKind::Memdx(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    Memdx(MemdxError),
    Query(QueryError),
    Search(SearchError),
    Analytics(AnalyticsError),
    Http(HttpError),
    Mgmt(MgmtError),
    VbucketMapOutdated,
    #[non_exhaustive]
    InvalidArgument {
        msg: String,
        arg: Option<String>,
    },
    #[non_exhaustive]
    EndpointNotKnown {
        endpoint: String,
    },
    InvalidVbucket {
        requested_vb_id: u16,
        num_vbuckets: usize,
    },
    InvalidReplica {
        requested_replica: u32,
        num_servers: usize,
    },
    NoEndpointsAvailable,
    Shutdown,
    NoBucket,
    IllegalState {
        msg: String,
    },
    NoVbucketMap,
    #[non_exhaustive]
    NoServerAssigned {
        requested_vb_id: u16,
    },
    #[non_exhaustive]
    CollectionManifestOutdated {
        manifest_uid: u64,
        server_manifest_uid: u64,
    },
    #[non_exhaustive]
    Message {
        msg: String,
    },
    #[non_exhaustive]
    ServiceNotAvailable {
        service: ServiceType,
    },
    #[non_exhaustive]
    FeatureNotAvailable {
        feature: String,
        msg: String,
    },
    #[non_exhaustive]
    Compression {
        msg: String,
    },
    #[non_exhaustive]
    Internal {
        msg: String,
    },
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::VbucketMapOutdated => write!(f, "vbucket map outdated"),
            ErrorKind::InvalidArgument { msg, arg } => {
                if let Some(arg) = arg {
                    write!(f, "invalid argument {}: {}", arg, msg)
                } else {
                    write!(f, "invalid argument: {}", msg)
                }
            }
            ErrorKind::Memdx(err) => write!(f, "{}", err),
            ErrorKind::Query(err) => write!(f, "{}", err),
            ErrorKind::Search(err) => write!(f, "{}", err),
            ErrorKind::Analytics(err) => write!(f, "{}", err),
            ErrorKind::Http(err) => write!(f, "{}", err),
            ErrorKind::Mgmt(err) => write!(f, "{}", err),
            ErrorKind::EndpointNotKnown { endpoint } => {
                write!(f, "endpoint not known: {}", endpoint)
            }
            ErrorKind::NoEndpointsAvailable => write!(f, "no endpoints available"),
            ErrorKind::Shutdown => write!(f, "shutdown"),
            ErrorKind::NoBucket => write!(f, "no bucket selected"),
            ErrorKind::IllegalState { msg } => write!(f, "illegal state: {}", msg),
            ErrorKind::NoVbucketMap => write!(f, "invalid vbucket map"),
            ErrorKind::CollectionManifestOutdated {
                manifest_uid,
                server_manifest_uid,
            } => {
                write!(
                    f,
                    "collection manifest outdated: our manifest uid: {}, server manifest uid: {}",
                    manifest_uid, server_manifest_uid
                )
            }
            ErrorKind::Message { msg } => write!(f, "{}", msg),
            ErrorKind::ServiceNotAvailable { service } => {
                write!(f, "service not available: {}", service)
            }
            ErrorKind::FeatureNotAvailable { feature, msg } => {
                write!(f, "feature not available: {}, {}", feature, msg)
            }
            ErrorKind::Internal { msg } => write!(f, "internal error: {}", msg),
            ErrorKind::NoServerAssigned { requested_vb_id } => {
                write!(f, "no server assigned for vbucket id: {}", requested_vb_id)
            }
            ErrorKind::InvalidVbucket {
                requested_vb_id,
                num_vbuckets,
            } => write!(
                f,
                "invalid vbucket id: {}, num vbuckets: {}",
                requested_vb_id, num_vbuckets
            ),
            ErrorKind::InvalidReplica {
                requested_replica,
                num_servers,
            } => write!(
                f,
                "invalid replica: {}, num servers: {}",
                requested_replica, num_servers
            ),
            ErrorKind::Compression { msg } => write!(f, "compression error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub struct MemdxError {
    source: memdx::error::Error,
    dispatched_to: Option<String>,
    dispatched_from: Option<String>,
}

impl MemdxError {
    pub(crate) fn new(source: memdx::error::Error) -> Self {
        Self {
            source,
            dispatched_to: None,
            dispatched_from: None,
        }
    }

    pub(crate) fn with_dispatched_to(mut self, dispatched_to: impl Into<String>) -> Self {
        self.dispatched_to = Some(dispatched_to.into());
        self
    }

    pub(crate) fn with_dispatched_from(mut self, dispatched_from: impl Into<String>) -> Self {
        self.dispatched_from = Some(dispatched_from.into());
        self
    }
}

impl Deref for MemdxError {
    type Target = memdx::error::Error;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl Display for MemdxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)?;
        if let Some(ref dispatched_to) = self.dispatched_to {
            write!(f, ", dispatched to: {}", dispatched_to)?;
        }
        if let Some(ref dispatched_from) = self.dispatched_from {
            write!(f, ", dispatched from: {}", dispatched_from)?;
        }
        Ok(())
    }
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
