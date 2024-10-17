use crate::error;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Serialize, Serializer};

#[derive(Debug, PartialEq)]
pub enum DataType {
    Unknown,
    Json,
    Binary,
    String,
}

#[derive(Debug, PartialEq)]
pub enum CompressionType {
    UnknownCompression,
    NoCompression,
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

pub fn decode_common_flags(flags: u32) -> (DataType, CompressionType) {
    // Check for legacy flags
    let mut flags = if flags & CF_MASK == 0 {
        // Legacy Flags
        if flags == LF_JSON {
            // Legacy JSON
            CF_FMT_JSON
        } else {
            return (DataType::Unknown, CompressionType::UnknownCompression);
        }
    } else {
        flags
    };

    let value_type = if flags & CF_FMT_MASK == CF_FMT_BINARY {
        DataType::Binary
    } else if flags & CF_FMT_MASK == CF_FMT_STRING {
        DataType::String
    } else if flags & CF_FMT_MASK == CF_FMT_JSON {
        DataType::Json
    } else {
        DataType::Unknown
    };

    let compression = if flags & CF_CMPR_MASK == CF_CMPR_NONE {
        CompressionType::NoCompression
    } else {
        CompressionType::UnknownCompression
    };

    (value_type, compression)
}

pub trait Transcoder {
    fn encode<T: Serialize>(&self, value: T) -> error::Result<(Bytes, u32)>;
    fn decode<T: DeserializeOwned>(&self, value: &Bytes, flags: u32) -> error::Result<T>;
}

pub struct DefaultTranscoder {}

impl Transcoder for DefaultTranscoder {
    fn encode<T: Serialize>(&self, value: T) -> error::Result<(Bytes, u32)> {
        Ok((
            Bytes::from(serde_json::to_vec(&value)?),
            encode_common_flags(DataType::Json),
        ))
    }

    fn decode<T: DeserializeOwned>(&self, value: &Bytes, flags: u32) -> error::Result<T> {
        let (value_type, compression) = decode_common_flags(flags);

        if compression != CompressionType::NoCompression {
            return Err(error::Error {
                msg: "expected value to not be compressed".into(),
            });
        }

        if value_type == DataType::Json {
            return Ok(serde_json::from_slice(value)?);
        }

        Err(error::Error {
            msg: "datatype not supported by this trancoder".to_string(),
        })
    }
}
