use crate::transcoding::{decode_common_flags, encode_common_flags, DataType, RawValue};
use bytes::Bytes;
use serde::Serialize;

pub fn encode<T: Serialize>(value: T) -> crate::error::Result<RawValue> {
    let content = Bytes::from(
        serde_json::to_vec(&value).map_err(|e| crate::error::Error { msg: e.to_string() })?,
    );
    let flags = encode_common_flags(DataType::Json);

    Ok(RawValue { content, flags })
}

pub fn decode<T: serde::de::DeserializeOwned>(value: &RawValue) -> crate::error::Result<T> {
    let datatype = decode_common_flags(value.flags);
    if datatype != DataType::Json {
        return Err(crate::error::Error {
            msg: "datatype not supported".to_string(),
        });
    }

    serde_json::from_slice(&value.content).map_err(|e| crate::error::Error { msg: e.to_string() })
}
