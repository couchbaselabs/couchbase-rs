use std::fmt::Display;

use crate::error::CoreError::{Dispatch, Placeholder, PlaceholderMemdxWrapper};
use crate::memdx::error::MemdxError;

#[derive(thiserror::Error, Debug, Eq, PartialEq)]
pub enum CoreError {
    #[error("Dispatch error {0}")]
    Dispatch(MemdxError),
    #[error("Placeholder error {0}")]
    Placeholder(String),
    #[error("Placeholder memdx wrapper error {0}")]
    PlaceholderMemdxWrapper(MemdxError),
}

impl From<MemdxError> for CoreError {
    fn from(value: MemdxError) -> Self {
        match value {
            MemdxError::Dispatch(_) => Dispatch(value),
            _ => PlaceholderMemdxWrapper(value),
        }
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(value: serde_json::Error) -> Self {
        Placeholder(value.to_string())
    }
}
