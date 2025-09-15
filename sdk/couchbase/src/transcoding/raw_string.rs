use crate::transcoding::{decode_common_flags, encode_common_flags, DataType};

pub fn encode(value: &str) -> crate::error::Result<(&[u8], u32)> {
    Ok((value.as_bytes(), encode_common_flags(DataType::String)))
}

pub fn decode(value: &[u8], flags: u32) -> crate::error::Result<&str> {
    let datatype = decode_common_flags(flags);
    if datatype != DataType::String {
        return Err(crate::error::Error::other_failure("datatype not supported"));
    }

    str::from_utf8(value)
        .map_err(|e| crate::error::Error::other_failure(format!("invalid UTF-8: {e}")))
}
