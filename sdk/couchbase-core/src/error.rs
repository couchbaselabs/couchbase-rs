use std::fmt::Display;

use crate::memdx::error::Error;

#[derive(Debug)]
pub struct CoreError {
    pub msg: String,
}

impl From<Error> for CoreError {
    fn from(value: Error) -> Self {
        Self {
            msg: value.to_string(),
        }
    }
}
