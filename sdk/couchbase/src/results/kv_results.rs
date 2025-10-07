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

use crate::mutation_state::MutationToken;
use crate::subdoc::lookup_in_specs::LookupInOpType;
use crate::{error, transcoding};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub(crate) content: Vec<u8>,
    pub(crate) flags: u32,
    pub(crate) cas: u64,
    pub(crate) expiry_time: Option<DateTime<Utc>>,
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

    pub fn expiry_time(&self) -> Option<&DateTime<Utc>> {
        self.expiry_time.as_ref()
    }
}

impl From<couchbase_core::results::kv::GetResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
        }
    }
}

impl From<couchbase_core::results::kv::GetAndTouchResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetAndTouchResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
        }
    }
}

impl From<couchbase_core::results::kv::GetAndLockResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetAndLockResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
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

impl From<couchbase_core::results::kv::GetMetaResult> for ExistsResult {
    fn from(result: couchbase_core::results::kv::GetMetaResult) -> Self {
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

impl From<couchbase_core::results::kv::TouchResult> for TouchResult {
    fn from(result: couchbase_core::results::kv::TouchResult) -> Self {
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
        serde_json::from_slice(&content).map_err(error::Error::decoding_failure_from_serde)
    }

    pub fn content_as_raw(&self, lookup_index: usize) -> error::Result<Bytes> {
        if lookup_index >= self.entries.len() {
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of lookups",
            ));
        }

        let entry = self
            .entries
            .get(lookup_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        if entry.op == LookupInOpType::Exists {
            let res = self.exists(lookup_index)?;
            let val = Bytes::from(
                serde_json::to_vec(&res).map_err(error::Error::decoding_failure_from_serde)?,
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
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of lookups",
            ));
        }

        let entry = self
            .entries
            .get(lookup_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        if let Some(err) = &entry.error {
            return match err.kind() {
                error::ErrorKind::PathNotFound => Ok(false),
                _ => Err(err.clone()),
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

        serde_json::from_slice(&content).map_err(error::Error::decoding_failure_from_serde)
    }

    pub fn content_as_raw(&self, mutate_index: usize) -> error::Result<Bytes> {
        if mutate_index >= self.entries.len() {
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of operations",
            ));
        }

        let entry = self
            .entries
            .get(mutate_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        Ok(entry.value.clone().unwrap_or_default())
    }
}
