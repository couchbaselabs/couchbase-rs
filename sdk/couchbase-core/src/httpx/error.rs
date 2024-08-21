use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
#[error("{kind}")]
#[non_exhaustive]
pub struct Error {
    /// Taken from serde_json: This `Box` allows us to keep the size of `Error` as small as possible.
    /// A larger `Error` type was substantially slower due to all the functions
    /// that pass around `Result<T, Error>`.
    pub kind: Box<ErrorKind>,
}

#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("Connect failed {msg}")]
    Connect { msg: String },
    #[error("Json error {msg}")]
    #[non_exhaustive]
    Json { msg: String },
    #[error("Generic error {msg}")]
    Generic { msg: String },
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

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        ErrorKind::Generic {
            msg: err.to_string(),
        }
        .into()
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(err: serde_json::error::Error) -> Self {
        ErrorKind::Json {
            msg: err.to_string(),
        }
        .into()
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        ErrorKind::Generic {
            msg: err.to_string(),
        }
        .into()
    }
}
