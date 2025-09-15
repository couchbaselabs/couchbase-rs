use crate::transcoding::{decode_common_flags, encode_common_flags, DataType};

pub fn encode<T: AsRef<[u8]>>(value: &T) -> crate::error::Result<(&[u8], u32)> {
    Ok((value.as_ref(), encode_common_flags(DataType::Json)))
}

pub fn decode(bytes: &[u8], flags: u32) -> crate::error::Result<&[u8]> {
    let datatype = decode_common_flags(flags);
    if datatype != DataType::Json {
        return Err(crate::error::Error::other_failure("datatype not supported"));
    }
    Ok(bytes)
}
