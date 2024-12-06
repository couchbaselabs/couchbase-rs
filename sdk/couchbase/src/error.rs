pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Error {
    pub msg: String,
}

impl From<couchbase_core::memdx::error::SubdocError> for Error {
    fn from(value: couchbase_core::memdx::error::SubdocError) -> Self {
        Self {
            msg: value.to_string(),
        }
    }
}

impl From<couchbase_core::error::Error> for Error {
    fn from(error: couchbase_core::error::Error) -> Self {
        Self {
            msg: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            msg: value.to_string(),
        }
    }
}

impl From<couchbase_connstr::error::Error> for Error {
    fn from(value: couchbase_connstr::error::Error) -> Self {
        Self {
            msg: value.to_string(),
        }
    }
}
