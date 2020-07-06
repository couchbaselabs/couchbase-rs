use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;
use std::io;

use super::protocol::HEADER_SIZE;

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
