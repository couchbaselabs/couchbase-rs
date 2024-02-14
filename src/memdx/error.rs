use std::fmt::{Display, Formatter};
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Dispatch failed {0}")]
    Dispatch(io::Error),
    #[error("Request cancelled {0}")]
    Cancelled(CancellationErrorKind),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancellationErrorKind {
    Timeout,
    RequestCancelled,
}

impl Display for CancellationErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            CancellationErrorKind::Timeout => "timeout",
            CancellationErrorKind::RequestCancelled => "request cancelled",
        };

        write!(f, "{}", txt)
    }
}
