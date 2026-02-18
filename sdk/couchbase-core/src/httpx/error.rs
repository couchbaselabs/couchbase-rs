/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::tracingcomponent::MetricsName;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    inner: Box<ErrorImpl>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)
    }
}

impl StdError for Error {}

impl Error {
    pub(crate) fn new_message_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Message(msg.into()),
            }),
        }
    }

    pub(crate) fn new_connection_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Connection { msg: msg.into() },
            }),
        }
    }

    pub(crate) fn new_decoding_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::Decoding(msg.into()),
            }),
        }
    }

    pub(crate) fn new_request_error(msg: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorImpl {
                kind: ErrorKind::SendRequest(msg.into()),
            }),
        }
    }

    pub fn is_connection_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Connection { .. })
    }

    pub fn is_decoding_error(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Decoding { .. })
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorImpl {
    kind: ErrorKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    #[non_exhaustive]
    Connection {
        msg: String,
    },
    Decoding(String),
    Message(String),
    SendRequest(String),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection { msg } => write!(f, "connection error {msg}"),
            Self::Decoding(msg) => write!(f, "decoding error: {msg}"),
            Self::Message(msg) => write!(f, "{msg}"),
            Self::SendRequest(msg) => write!(f, "send request failed error: {msg}"),
        }
    }
}

impl MetricsName for Error {
    fn metrics_name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::Connection { .. } => "httpx.Connection",
            ErrorKind::Decoding(_) => "httpx.Decoding",
            ErrorKind::Message(_) => "httpx._OTHER",
            ErrorKind::SendRequest(_) => "httpx.SendRequest",
        }
    }
}
