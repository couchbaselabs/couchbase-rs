use std::fmt::Display;

use crate::error::CoreError::{Dispatch, Placeholder, PlaceholderMemdxWrapper};
use crate::memdx::error::{Error, ErrorKind};

#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error("Dispatch error {0}")]
    Dispatch(Error),
    #[error("Placeholder error {0}")]
    Placeholder(String),
    #[error("Placeholder memdx wrapper error {0}")]
    PlaceholderMemdxWrapper(Error),
}

impl From<Error> for CoreError {
    fn from(value: Error) -> Self {
        match value.kind.as_ref() {
            ErrorKind::Dispatch(_) => Dispatch(value),
            _ => PlaceholderMemdxWrapper(value),
        }
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(value: serde_json::Error) -> Self {
        Placeholder(value.to_string())
    }
}
