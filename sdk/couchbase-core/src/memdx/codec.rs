/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::memdx::error::Error;
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::status::Status;

pub const HEADER_SIZE: usize = 24;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct KeyValueCodec(());

impl Decoder for KeyValueCodec {
    type Item = ResponsePacket;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let buf_len = buf.len();

        if buf_len < HEADER_SIZE {
            return Ok(None);
        }

        let total_body_len = match buf[8..12].try_into() {
            Ok(v) => u32::from_be_bytes(v),
            Err(e) => {
                return Err(Error::new_protocol_error("failed to read total body length").with(e));
            }
        } as usize;

        if buf_len < (HEADER_SIZE + total_body_len) {
            buf.reserve(HEADER_SIZE + total_body_len);
            return Ok(None);
        }

        let mut slice = buf.split_to(HEADER_SIZE + total_body_len);

        // 0
        let magic = Magic::try_from(slice.get_u8())?;
        let flexible = magic.is_extended();

        // 1
        let opcode = OpCode::try_from(slice.get_u8())?;

        let flexible_extras_len = if flexible {
            // 2
            slice.get_u8()
        } else {
            0
        } as usize;

        let key_len = if flexible {
            // 3
            slice.get_u8() as u16
        } else {
            // 2, 3
            slice.get_u16()
        } as usize;

        // 4
        let extras_len = slice.get_u8() as usize;
        // 5
        let datatype = slice.get_u8();
        // 6, 7
        let status = Status::from(slice.get_u16());

        // 8, 9
        let total_body_len = slice.get_u32() as usize;
        // 10, 11, 12, 13
        let opaque = slice.get_u32();
        // 14, 15, 16, 17, 18, 19, 20, 21
        let cas = slice.get_u64();
        let body_len = total_body_len - key_len - extras_len - flexible_extras_len;

        let mut packet = ResponsePacket::new(magic, opcode, datatype, status, opaque);
        packet.cas = Some(cas);

        let mut payload_pos = 0;

        if flexible_extras_len > 0 {
            packet.framing_extras =
                Some(slice[payload_pos..(payload_pos + flexible_extras_len)].to_vec());
            payload_pos += flexible_extras_len;
        }

        if extras_len > 0 {
            packet.extras = Some(slice[payload_pos..(payload_pos + extras_len)].to_vec());
            payload_pos += extras_len;
        };

        if key_len > 0 {
            packet.key = Some(slice[payload_pos..(payload_pos + key_len)].to_vec());
            payload_pos += key_len;
        };

        if body_len > 0 {
            packet.value = Some(slice[payload_pos..].to_vec());
        };

        Ok(Some(packet))
    }
}

impl Encoder<RequestPacket<'_>> for KeyValueCodec {
    type Error = Error;

    fn encode(&mut self, item: RequestPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let key = item.key;
        let extras = item.extras;
        let framing_extras = item.framing_extras;
        let body = item.value;

        let key_size = if let Some(k) = &key { k.len() } else { 0 };
        let extras_size = if let Some(e) = &extras { e.len() } else { 0 };
        let framing_extras_size = if let Some(e) = framing_extras {
            e.len()
        } else {
            0
        };
        let body_size = if let Some(b) = &body { b.len() } else { 0 };

        let total_body_size = key_size + extras_size + framing_extras_size + body_size;

        dst.reserve(HEADER_SIZE + total_body_size);

        dst.put_u8(item.magic.into());
        dst.put_u8(item.op_code.into());
        if framing_extras.is_some() {
            if key_size > u8::MAX as usize {
                return Err(Error::new_invalid_argument_error(
                    "key size too large",
                    "key".to_string(),
                ));
            }

            if framing_extras_size > u8::MAX as usize {
                return Err(Error::new_invalid_argument_error(
                    "frame extras too large",
                    "frame extras".to_string(),
                ));
            }

            dst.put_u8(framing_extras_size as u8);
            dst.put_u8(key_size as u8);
        } else {
            if key_size > u16::MAX as usize {
                return Err(Error::new_invalid_argument_error(
                    "key size too large",
                    "key".to_string(),
                ));
            }

            dst.put_u16(key_size as u16);
        }
        dst.put_u8(extras_size as u8);
        dst.put_u8(item.datatype);
        dst.put_u16(item.vbucket_id.unwrap_or_default());
        dst.put_u32(total_body_size as u32);
        dst.put_u32(item.opaque.unwrap_or_default());
        dst.put_u64(item.cas.unwrap_or_default());

        if let Some(framing_extras) = framing_extras {
            dst.extend_from_slice(framing_extras);
        }

        if let Some(extras) = extras {
            dst.extend_from_slice(extras);
        }

        if let Some(key) = key {
            dst.extend_from_slice(key);
        }

        if let Some(body) = body {
            dst.extend_from_slice(body);
        }

        Ok(())
    }
}
