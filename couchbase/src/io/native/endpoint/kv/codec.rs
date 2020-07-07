//! Encoding and decoding facilities for the kv protocol.

use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;
use std::io;

use super::protocol::HEADER_SIZE;

/// The `KeyValueCodec` aggregates byte chunks into full packets at their boundaries
/// on decoding.
///
/// This is important, since over TCP the packets can arrive in various chunk sizes
/// which are not necessarily exactly at the memcache binary protocol boundaries. On
/// The encoding side though the codec just sends along the data, since there is no
/// need to split up anything.
///
/// In the future this codec could also do basic validation of the payload, but at
/// the moment this is not implemented.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct KeyValueCodec(());

impl KeyValueCodec {
    pub fn new() -> Self {
        KeyValueCodec(())
    }
}

impl Decoder for KeyValueCodec {
    type Item = BytesMut;
    type Error = io::Error;

    /// Decodes packets onto their correct protocol boundaries.
    ///
    /// The algorithm is rather simple, and copied from the current java implementation.
    /// It first checks if we have at least a 24 byte header (HEADER_SIZE) available to
    /// figure out how many bytes the total boy length is. Once the header is received
    /// we extract the body length and then make sure we have the full packet around
    /// so header size plus total body length. Once this is the case, split at that
    /// boundary and return it to the caller.
    ///
    /// Subsequent packets will be left in the input buffer and consumed during the
    /// next iterations.
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        let buf_len = buf.len();

        if buf_len < HEADER_SIZE {
            return Ok(None);
        }

        let total_body_len = match buf[8..12].try_into() {
            Ok(v) => u32::from_be_bytes(v),
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        } as usize;

        if buf_len < total_body_len {
            return Ok(None);
        }

        Ok(Some(buf.split_to(HEADER_SIZE + total_body_len)))
    }
}

impl Encoder<Bytes> for KeyValueCodec {
    type Error = io::Error;

    /// The encoder just reserves the bytes on the output buffer and writes
    /// the data completely into it in one shot.
    ///
    /// No chunking or boundary checks are performed during encode as opposed
    /// to the decode logic. The upper levels are responsible for sending
    /// correctly formatted requests and responses downstream.
    fn encode(&mut self, data: Bytes, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.reserve(data.len());
        buf.put(data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::super::protocol::*;
    use super::*;

    #[test]
    fn ignores_empty_input() {
        let mut codec = KeyValueCodec::new();

        let mut input = BytesMut::new();
        let result = codec.decode(&mut input);
        assert_eq!(None, result.unwrap());
        assert!(input.is_empty());
    }

    #[test]
    fn waits_until_header_complete() {
        let mut codec = KeyValueCodec::new();

        let mut input = response(Opcode::Noop, 0, 0, 0, 0, None, None, None);
        let expected = input.clone();
        let trailing = input.split_off(12);

        let result = codec.decode(&mut input);
        assert_eq!(None, result.unwrap());

        input.put(trailing);
        let result = codec.decode(&mut input);
        assert_eq!(Some(expected), result.unwrap());
        assert!(input.is_empty());
    }

    #[test]
    fn waits_for_payload_if_needed() {
        let mut codec = KeyValueCodec::new();

        let key = Bytes::from("Key");
        let mut input = response(Opcode::Noop, 0, 0, 0, 0, Some(key), None, None);
        let expected = input.clone();
        let trailing = input.split_off(22);

        let result = codec.decode(&mut input);
        assert_eq!(None, result.unwrap());

        input.put(trailing);
        let result = codec.decode(&mut input);
        assert_eq!(Some(expected), result.unwrap());
        assert!(input.is_empty());
    }

    #[test]
    fn splits_packets_on_boundaries() {
        let mut codec = KeyValueCodec::new();

        let response1 = response(Opcode::Noop, 0, 0, 0, 0, None, None, None);
        let response2 = response(Opcode::Get, 0, 0, 0, 0, None, None, None);
        let mut input = BytesMut::with_capacity(response1.len() + response2.len());
        input.put(response1);
        input.put(response2);

        let result = codec.decode(&mut input);
        assert_eq!(HEADER_SIZE, result.unwrap().unwrap().len());
        assert_eq!(HEADER_SIZE, input.len());

        let result = codec.decode(&mut input);
        assert_eq!(HEADER_SIZE, result.unwrap().unwrap().len());
        assert!(input.is_empty());
    }
}
