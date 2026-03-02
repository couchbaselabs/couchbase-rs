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

//! Transcoding utilities for encoding and decoding document content.
//!
//! The SDK uses "common flags" to identify the data type of stored documents. This module
//! provides sub-modules for different encoding formats:
//!
//! | Module | Data Type | Description |
//! |--------|-----------|-------------|
//! | [`json`] | JSON | Serialize/deserialize via `serde_json` (default for most operations) |
//! | [`raw_binary`] | Binary | Store/retrieve raw binary data |
//! | [`raw_json`] | JSON | Store/retrieve pre-encoded JSON bytes |
//! | [`raw_string`] | String | Store/retrieve UTF-8 string data |
//!
//! Most users will not need to interact with this module directly — the standard
//! `Collection` methods handle JSON transcoding automatically. Use the `*_raw` method
//! variants (e.g. [`Collection::upsert_raw`](crate::collection::Collection)) when you
//! need custom transcoding.

pub mod json;
pub mod raw_binary;
pub mod raw_json;
pub mod raw_string;

use serde::de::DeserializeOwned;
use serde::{Serialize, Serializer};

/// Identifies the data type of a document's content based on common flags.
#[derive(Debug, PartialEq, Clone, Hash, Ord, PartialOrd, Eq)]
#[non_exhaustive]
pub enum DataType {
    /// The data type is unknown or unrecognized.
    Unknown,
    /// JSON-encoded data.
    Json,
    /// Raw binary data.
    Binary,
    /// UTF-8 string data.
    String,
}

const CF_MASK: u32 = 0xFF000000;
const CF_FMT_MASK: u32 = 0x0F000000;
const CF_CMPR_MASK: u32 = 0xE0000000;

const CF_FMT_JSON: u32 = 2 << 24;
const CF_FMT_BINARY: u32 = 3 << 24;
const CF_FMT_STRING: u32 = 4 << 24;

const CF_CMPR_NONE: u32 = 0 << 29;
const LF_JSON: u32 = 0;

pub fn encode_common_flags(value_type: DataType) -> u32 {
    let mut flags: u32 = 0;

    match value_type {
        DataType::Json => flags |= CF_FMT_JSON,
        DataType::Binary => flags |= CF_FMT_BINARY,
        DataType::String => flags |= CF_FMT_STRING,
        DataType::Unknown => {}
    }

    flags
}

pub fn decode_common_flags(flags: u32) -> DataType {
    // Check for legacy flags
    let mut flags = if flags & CF_MASK == 0 {
        // Legacy Flags
        if flags == LF_JSON {
            // Legacy JSON
            CF_FMT_JSON
        } else {
            return DataType::Unknown;
        }
    } else {
        flags
    };

    if flags & CF_FMT_MASK == CF_FMT_BINARY {
        DataType::Binary
    } else if flags & CF_FMT_MASK == CF_FMT_STRING {
        DataType::String
    } else if flags & CF_FMT_MASK == CF_FMT_JSON {
        DataType::Json
    } else {
        DataType::Unknown
    }
}
