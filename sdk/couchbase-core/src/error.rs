use std::fmt::{Display, Formatter};

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

impl Display for CoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}
