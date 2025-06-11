use crate::transcoding::{decode_common_flags, encode_common_flags, DataType};

pub fn encode(value: &[u8]) -> crate::error::Result<(&[u8], u32)> {
    Ok((value, encode_common_flags(DataType::Binary)))
}

pub fn decode(value: &[u8], flags: u32) -> crate::error::Result<&[u8]> {
    let datatype = decode_common_flags(flags);
    if datatype != DataType::Binary {
        return Err(crate::error::Error::other_failure("datatype not supported"));
    }

    Ok(value)
}
