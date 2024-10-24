use crate::transcoding::{decode_common_flags, encode_common_flags, DataType, RawValue};
use bytes::Bytes;

pub fn encode(value: Bytes) -> crate::error::Result<RawValue> {
    Ok(RawValue {
        content: value,
        flags: encode_common_flags(DataType::Binary),
    })
}

pub fn decode(value: RawValue) -> crate::error::Result<Bytes> {
    let datatype = decode_common_flags(value.flags);
    if datatype != DataType::Binary {
        return Err(crate::error::Error {
            msg: "datatype not supported".to_string(),
        });
    }

    Ok(value.content)
}
