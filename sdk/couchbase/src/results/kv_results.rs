use crate::mutation_state::MutationToken;
use crate::transcoding::RawValue;
use crate::{error, transcoding};
use bytes::Bytes;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GetResult {
    content: Bytes,
    flags: u32,
    cas: u64,
}

impl GetResult {
    pub fn content_as<V: DeserializeOwned>(self) -> error::Result<V> {
        let content = self.content_as_raw();
        transcoding::json::decode(&content)
    }

    pub fn content_as_raw(self) -> RawValue {
        RawValue {
            content: self.content,
            flags: self.flags,
        }
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::crudresults::GetResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetResult) -> Self {
        Self {
            content: Bytes::from(result.value),
            flags: result.flags,
            cas: result.cas,
        }
    }
}

impl From<couchbase_core::crudresults::GetAndTouchResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetAndTouchResult) -> Self {
        Self {
            content: Bytes::from(result.value),
            flags: result.flags,
            cas: result.cas,
        }
    }
}

impl From<couchbase_core::crudresults::GetAndLockResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetAndLockResult) -> Self {
        Self {
            content: Bytes::from(result.value),
            flags: result.flags,
            cas: result.cas,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ExistsResult {
    exists: bool,
    cas: u64,
}

impl ExistsResult {
    pub fn exists(&self) -> bool {
        self.exists
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::crudresults::GetMetaResult> for ExistsResult {
    fn from(result: couchbase_core::crudresults::GetMetaResult) -> Self {
        Self {
            exists: !result.deleted,
            cas: result.cas,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TouchResult {
    cas: u64,
}

impl TouchResult {
    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::crudresults::TouchResult> for TouchResult {
    fn from(result: couchbase_core::crudresults::TouchResult) -> Self {
        Self { cas: result.cas }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutationResult {
    pub(crate) cas: u64,
    pub(crate) mutation_token: Option<MutationToken>,
}

impl MutationResult {
    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> &Option<MutationToken> {
        &self.mutation_token
    }
}
