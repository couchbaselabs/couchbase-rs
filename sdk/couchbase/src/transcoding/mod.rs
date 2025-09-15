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
