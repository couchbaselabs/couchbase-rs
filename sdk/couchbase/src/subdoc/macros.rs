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

//! Server-side macro constants for sub-document extended attributes (xattrs).
//!
//! These macros are expanded by the server at operation time. Use [`LookupInMacros`] as paths
//! in [`LookupInSpec::get`](super::lookup_in_specs::LookupInSpec::get) to read document metadata,
//! and [`MutateInMacros`] as values in [`MutateInSpec`](super::mutate_in_specs::MutateInSpec)
//! operations to inject server-generated values.

use serde::{Serialize, Serializer};

/// Server-side macros that can be used as paths in [`LookupInSpec::get`](super::lookup_in_specs::LookupInSpec::get)
/// to retrieve document metadata.
///
/// Convert to a string path with `.into()`.
///
/// # Example
///
/// ```rust
/// use couchbase::subdoc::macros::LookupInMacros;
/// use couchbase::subdoc::lookup_in_specs::{LookupInSpec, GetSpecOptions};
///
/// let spec = LookupInSpec::get(
///     LookupInMacros::CAS,
///     GetSpecOptions::new().xattr(true),
/// );
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum LookupInMacros {
    /// The full document metadata object.
    Document,
    /// The document's expiry time.
    ExpiryTime,
    /// The document's CAS value.
    CAS,
    /// The document's sequence number.
    SeqNo,
    /// The last modified timestamp.
    LastModified,
    /// Whether the document has been deleted (tombstone).
    IsDeleted,
    /// The size of the document value in bytes.
    ValueSizeBytes,
    /// The document's revision ID.
    RevId,
}

impl From<LookupInMacros> for String {
    fn from(macro_: LookupInMacros) -> Self {
        match macro_ {
            LookupInMacros::Document => "$document".to_string(),
            LookupInMacros::ExpiryTime => "$document.exptime".to_string(),
            LookupInMacros::CAS => "$document.CAS".to_string(),
            LookupInMacros::SeqNo => "$document.seqno".to_string(),
            LookupInMacros::LastModified => "$document.last_modified".to_string(),
            LookupInMacros::IsDeleted => "$document.deleted".to_string(),
            LookupInMacros::ValueSizeBytes => "$document.value_bytes".to_string(),
            LookupInMacros::RevId => "$document.revid".to_string(),
        }
    }
}

/// Server-side macros that can be used as values in [`MutateInSpec`](super::mutate_in_specs::MutateInSpec)
/// operations. The server will expand them to the actual value at mutation time.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum MutateInMacros {
    /// Expands to the CAS value of the mutation.
    #[serde(rename = "${Mutation.CAS}")]
    CAS,
    /// Expands to the sequence number of the mutation.
    #[serde(rename = "${Mutation.seqno}")]
    SeqNo,
    /// Expands to the CRC32c of the document value after mutation.
    #[serde(rename = "${Mutation.value_crc32c}")]
    ValueCrc32c,
}

pub(crate) static MUTATE_IN_MACROS: &[&[u8]] = &[
    br#""${Mutation.CAS}""#,
    br#""${Mutation.seqno}""#,
    br#""${Mutation.value_crc32c}""#,
];
