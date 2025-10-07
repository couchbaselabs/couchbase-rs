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

pub mod json;
pub mod raw_binary;
pub mod raw_json;
pub mod raw_string;

use serde::de::DeserializeOwned;
use serde::{Serialize, Serializer};

#[derive(Debug, PartialEq, Clone, Hash, Ord, PartialOrd, Eq)]
#[non_exhaustive]
pub enum DataType {
    Unknown,
    Json,
    Binary,
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
