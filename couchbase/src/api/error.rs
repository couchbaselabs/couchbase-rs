use serde_json::Value;
use snafu::Snafu;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::{Debug, Error, Formatter};

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum CouchbaseError {
    #[snafu(display("A generic / unknown error happened: {}", ctx))]
    Generic { ctx: ErrorContext },
    #[snafu(display("Document with the given ID not found: {}", ctx))]
    DocumentNotFound { ctx: ErrorContext },
    #[snafu(display("Decoding the document content failed: {} {}", ctx, source))]
    DecodingFailure {
        ctx: ErrorContext,
        source: std::io::Error,
    },
    #[snafu(display("Encoding the document content failed: {} {}", ctx, source))]
    EncodingFailure {
        ctx: ErrorContext,
        source: std::io::Error,
    },
    #[snafu(display("The given argument is invalid: {}", ctx))]
    InvalidArgument { ctx: ErrorContext },
}

pub type CouchbaseResult<T, E = CouchbaseError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ErrorContext {
    inner: HashMap<String, Value>,
}

impl Default for ErrorContext {
    fn default() -> Self {
        ErrorContext {
            inner: HashMap::new(),
        }
    }
}

impl Display for ErrorContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}",
            serde_json::to_string(&self.inner).unwrap_or_else(|_| "".into())
        )
    }
}
