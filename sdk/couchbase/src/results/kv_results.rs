use crate::mutation_state::MutationToken;
use crate::subdoc::lookup_in_specs::LookupInOpType;
use crate::{error, transcoding};
use bytes::Bytes;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GetResult {
    content: Vec<u8>,
    flags: u32,
    cas: u64,
}

impl GetResult {
    pub fn content_as<V: DeserializeOwned>(&self) -> error::Result<V> {
        let (content, flags) = self.content_as_raw();
        transcoding::json::decode(content, flags)
    }

    pub fn content_as_raw(&self) -> (&[u8], u32) {
        (&self.content, self.flags)
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::crudresults::GetResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
        }
    }
}

impl From<couchbase_core::crudresults::GetAndTouchResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetAndTouchResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
        }
    }
}

impl From<couchbase_core::crudresults::GetAndLockResult> for GetResult {
    fn from(result: couchbase_core::crudresults::GetAndLockResult) -> Self {
        Self {
            content: result.value,
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
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct LookupInResultEntry {
    pub(crate) value: Option<Bytes>,
    pub(crate) error: Option<error::Error>,
    pub(crate) op: LookupInOpType,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LookupInResult {
    pub(crate) cas: u64,
    pub(crate) entries: Vec<LookupInResultEntry>,
    pub(crate) is_deleted: bool,
}

impl LookupInResult {
    pub fn content_as<V: DeserializeOwned>(&self, lookup_index: usize) -> error::Result<V> {
        let content = self.content_as_raw(lookup_index)?;
        serde_json::from_slice(&content).map_err(|e| error::Error { msg: e.to_string() })
    }

    pub fn content_as_raw(&self, lookup_index: usize) -> error::Result<Bytes> {
        if lookup_index >= self.entries.len() {
            return Err(error::Error {
                msg: "Index cannot be >= number of lookups".into(),
            });
        }

        let entry = self.entries.get(lookup_index).ok_or_else(|| error::Error {
            msg: "Index out of bounds".into(),
        })?;

        if entry.op == LookupInOpType::Exists {
            let res = self.exists(lookup_index)?;
            let val = Bytes::from(
                serde_json::to_vec(&res).map_err(|e| error::Error { msg: e.to_string() })?,
            );
            return Ok(val);
        }

        if let Some(err) = &entry.error {
            return Err(err.clone());
        }

        Ok(entry.value.clone().unwrap_or_default())
    }

    pub fn exists(&self, lookup_index: usize) -> error::Result<bool> {
        if lookup_index >= self.entries.len() {
            return Err(error::Error {
                msg: "Index cannot be >= number of lookups".into(),
            });
        }

        let entry = self.entries.get(lookup_index).ok_or_else(|| error::Error {
            msg: "Index out of bounds".into(),
        })?;

        if let Some(err) = &entry.error {
            return if err.msg.contains("subdoc path not found") {
                Ok(false)
            } else {
                Err(err.clone())
            };
        };

        Ok(true)
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutateInResultEntry {
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutateInResult {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub entries: Vec<MutateInResultEntry>,
}

impl MutateInResult {
    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> &Option<MutationToken> {
        &self.mutation_token
    }

    pub fn content_as<V: DeserializeOwned>(&self, mutate_index: usize) -> error::Result<V> {
        let content = self.content_as_raw(mutate_index)?;

        serde_json::from_slice(&content).map_err(|e| error::Error { msg: e.to_string() })
    }

    pub fn content_as_raw(&self, mutate_index: usize) -> error::Result<Bytes> {
        if mutate_index >= self.entries.len() {
            return Err(error::Error {
                msg: "Index cannot be >= number of operations".into(),
            });
        }

        let entry = self.entries.get(mutate_index).ok_or_else(|| error::Error {
            msg: "Index  out of bounds".into(),
        })?;

        Ok(entry.value.clone().unwrap_or_default())
    }
}
