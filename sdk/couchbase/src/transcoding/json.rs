use crate::transcoding::{decode_common_flags, encode_common_flags, DataType};
use serde::Serialize;

pub fn encode<T: Serialize>(value: T) -> crate::error::Result<(Vec<u8>, u32)> {
    let content = serde_json::to_vec(&value)
        .map_err(|e| crate::error::Error::other_failure(e.to_string()))?;
    let flags = encode_common_flags(DataType::Json);

    Ok((content, flags))
}

pub fn decode<T: serde::de::DeserializeOwned>(value: &[u8], flags: u32) -> crate::error::Result<T> {
    let datatype = decode_common_flags(flags);
    if datatype != DataType::Json {
        return Err(crate::error::Error::other_failure("datatype not supported"));
    }

    serde_json::from_slice(value).map_err(|e| crate::error::Error::other_failure(e.to_string()))
}
