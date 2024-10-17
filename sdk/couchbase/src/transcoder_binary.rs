use crate::error;
use crate::transcoder::{decode_common_flags, CompressionType, DataType, Transcoder};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_binary::binary_stream;

pub struct BinaryTranscoder {}

impl Transcoder for BinaryTranscoder {
    fn encode<T: Serialize>(&self, value: T) -> crate::error::Result<(Bytes, u32)> {
        Ok((
            Bytes::from(
                serde_binary::to_vec(&value, binary_stream::Endian::Little)
                    .map_err(|e| error::Error { msg: e.to_string() })?,
            ),
            crate::transcoder::encode_common_flags(DataType::Binary),
        ))
    }

    fn decode<T: DeserializeOwned>(&self, value: &Bytes, flags: u32) -> error::Result<T> {
        let (value_type, compression) = decode_common_flags(flags);

        if compression != CompressionType::NoCompression {
            return Err(error::Error {
                msg: "expected value to not be compressed".into(),
            });
        }

        if value_type == DataType::Binary {
            return serde_binary::from_slice(value, binary_stream::Endian::Little)
                .map_err(|e| error::Error { msg: e.to_string() });
        }

        Err(error::Error {
            msg: "datatype not supported by this trancoder".to_string(),
        })
    }
}
