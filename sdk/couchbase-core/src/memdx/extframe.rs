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

use crate::memdx::durability_level::{DurabilityLevel, DurabilityLevelSettings};
use crate::memdx::error;
use crate::memdx::error::Error;
use crate::memdx::ext_frame_code::{ExtReqFrameCode, ExtResFrameCode};
use bytes::BufMut;
use std::time::Duration;

pub(crate) fn decode_res_ext_frames(buf: &[u8]) -> error::Result<Option<Duration>> {
    let mut server_duration_data = None;

    iter_ext_frames(buf, |code, data| {
        if code == ExtResFrameCode::ServerDuration {
            server_duration_data = Some(decode_server_duration_ext_frame(data));
        }
    })?;

    if let Some(data) = server_duration_data {
        return Ok(Some(data?));
    }

    Ok(None)
}

pub fn decode_ext_frame(buf: &[u8]) -> error::Result<(ExtResFrameCode, &[u8], usize)> {
    if buf.is_empty() {
        return Err(Error::new_protocol_error(
            "empty value buffer when decoding ext frame",
        ));
    }

    let mut buf_pos = 0;

    let frame_header = buf[buf_pos];
    let mut u_frame_code = (frame_header & 0xF0) >> 4;
    let mut frame_code = ExtResFrameCode::from(u_frame_code as u16);
    let mut frame_len = frame_header & 0x0F;
    buf_pos += 1;

    if u_frame_code == 15 {
        if buf.len() < buf_pos + 1 {
            return Err(Error::new_protocol_error(
                "unexpected eof decoding ext frame",
            ));
        }

        let frame_code_ext = buf[buf_pos];
        u_frame_code = 15 + frame_code_ext;
        frame_code = ExtResFrameCode::from(u_frame_code as u16);
        buf_pos += 1;
    }

    if frame_len == 15 {
        if buf.len() < buf_pos + 1 {
            return Err(Error::new_protocol_error(
                "unexpected eof decoding ext frame",
            ));
        }

        let frame_len_ext = buf[buf_pos];
        frame_len = 15 + frame_len_ext;
        buf_pos += 1;
    }

    let u_frame_len = frame_len as usize;
    if buf.len() < buf_pos + u_frame_len {
        return Err(Error::new_protocol_error(
            "unexpected eof decoding ext frame",
        ));
    }

    let frame_body = &buf[buf_pos..buf_pos + u_frame_len];
    buf_pos += u_frame_len;

    Ok((frame_code, frame_body, buf_pos))
}

fn iter_ext_frames(buf: &[u8], mut cb: impl FnMut(ExtResFrameCode, &[u8])) -> error::Result<&[u8]> {
    if !buf.is_empty() {
        let (frame_code, frame_body, buf_pos) = decode_ext_frame(buf)?;

        cb(frame_code, frame_body);

        return Ok(&buf[buf_pos..]);
    }

    Ok(buf)
}

pub fn append_ext_frame(
    frame_code: ExtReqFrameCode,
    frame_body: &[u8],
    buf: &mut [u8],
    offset: &mut usize,
) -> error::Result<()> {
    let frame_len = frame_body.len();

    if *offset >= buf.len() {
        return Err(Error::new_invalid_argument_error(
            "buffer overflow",
            "ext frame".to_string(),
        ));
    }

    buf[*offset] = 0;
    let hdr_byte_ptr = *offset;
    *offset += 1;
    let u_frame_code: u16 = frame_code.into();

    if u_frame_code < 15 {
        buf[hdr_byte_ptr] |= ((u_frame_code & 0x0f) << 4) as u8;
    } else {
        if u_frame_code - 15 >= 15 {
            return Err(Error::new_invalid_argument_error(
                "ext frame code too large to encode",
                "ext frame".to_string(),
            ));
        }
        buf[hdr_byte_ptr] |= 0xF0;

        if *offset + 2 > buf.len() {
            return Err(Error::new_invalid_argument_error(
                "buffer overflow",
                "ext frame".to_string(),
            ));
        }
        buf[*offset..*offset + 2].copy_from_slice(&(u_frame_code.to_be_bytes()));
        *offset += 2;
    }

    if frame_len < 15 {
        buf[hdr_byte_ptr] |= (frame_len as u8) & 0xF;
    } else {
        if frame_len - 15 >= 15 {
            return Err(Error::new_invalid_argument_error(
                "ext frame len too large to encode",
                "ext frame".to_string(),
            ));
        }
        buf[hdr_byte_ptr] |= 0x0F;
        if *offset + 2 > buf.len() {
            return Err(Error::new_invalid_argument_error(
                "buffer overflow",
                "ext frame".to_string(),
            ));
        }
        buf[*offset..*offset + 2].copy_from_slice(&((frame_len - 15) as u16).to_be_bytes());
        *offset += 2;
    }

    if frame_len > 0 {
        if *offset + frame_len > buf.len() {
            return Err(Error::new_invalid_argument_error(
                "buffer overflow",
                "ext frame".to_string(),
            ));
        }
        buf[*offset..*offset + frame_len].copy_from_slice(frame_body);
        *offset += frame_len;
    }

    Ok(())
}

pub fn make_uleb128_32(collection_id: u32, buf: &mut [u8]) -> usize {
    let mut cid = collection_id;
    let mut count = 0;
    loop {
        let mut c: u8 = (cid & 0x7f) as u8;
        cid >>= 7;
        if cid != 0 {
            c |= 0x80;
        }

        buf[count] = c;
        count += 1;
        if c & 0x80 == 0 {
            break;
        }
    }

    count
}

pub fn encode_durability_ext_frame(
    level: DurabilityLevel,
    timeout: Option<Duration>,
) -> error::Result<Vec<u8>> {
    if timeout.is_none() {
        return Ok(vec![level.into()]);
    }

    let timeout = timeout.unwrap();

    let mut timeout_millis = timeout.as_millis();
    if timeout_millis > 65535 {
        return Err(Error::new_invalid_argument_error(
            "cannot encode durability timeout greater than 65535 milliseconds",
            "durability_level_timeout".to_string(),
        ));
    }

    if timeout_millis == 0 {
        timeout_millis = 1;
    }

    let mut buf = vec![level.into()];
    buf.put_u8((timeout_millis >> 8) as u8);
    buf.put_u8(timeout_millis as u8);

    Ok(buf)
}

pub(crate) fn decode_server_duration_ext_frame(mut data: &[u8]) -> error::Result<Duration> {
    if data.len() != 2 {
        return Err(Error::new_protocol_error(
            "invalid server duration ext frame length",
        ));
    }

    let dura_enc = ((data[0] as u32) << 8) | (data[1] as u32);
    let dura_micros = ((dura_enc as f32).powf(1.74) / 2.0).round();

    Ok(Duration::from_micros(dura_micros as u64))
}

pub(crate) fn decode_durability_level_ext_frame(
    data: &mut Vec<u8>,
) -> error::Result<DurabilityLevelSettings> {
    if data.len() == 1 {
        let durability = DurabilityLevel::from(data.remove(0));

        return Ok(DurabilityLevelSettings::new(durability));
    } else if data.len() == 3 {
        let durability = DurabilityLevel::from(data.remove(0));
        let timeout_millis = ((data.remove(0) as u32) << 8) | (data.remove(0) as u32);

        return Ok(DurabilityLevelSettings::new_with_timeout(
            durability,
            Duration::from_millis(timeout_millis as u64),
        ));
    }

    Err(Error::new_message_error(
        "invalid durability ext frame length",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memdx::durability_level::DurabilityLevel;
    use std::time::Duration;

    fn test_one_durability(
        l: DurabilityLevel,
        d: impl Into<Option<Duration>>,
        expected_bytes: &[u8],
    ) {
        let d = d.into();
        let data = encode_durability_ext_frame(l, d).expect("encode failed");
        assert_eq!(data, expected_bytes);

        let mut data_clone = data.clone();
        let settings = decode_durability_level_ext_frame(&mut data_clone).expect("decode failed");
        assert_eq!(settings.durability_level, l);

        let decoded_timeout = settings.timeout.unwrap_or(Duration::from_millis(0));
        if let Some(d) = d {
            let diff = (decoded_timeout.as_millis() as i64 - d.as_millis() as i64).abs();
            assert!(
                diff <= 1,
                "Expected relative difference less than 1ms, got {}",
                diff
            );
        } else {
            assert_eq!(0, decoded_timeout.as_millis() as i64);
        }
    }

    #[test]
    fn test_durability_ext_frame_majority_no_duration() {
        test_one_durability(DurabilityLevel::MAJORITY, None, &[0x01]);
    }

    #[test]
    fn test_durability_ext_frame_majority_persist_active_no_duration() {
        test_one_durability(DurabilityLevel::MAJORITY_AND_PERSIST_ACTIVE, None, &[0x02]);
    }

    #[test]
    fn test_durability_ext_frame_majority_duration_0() {
        test_one_durability(
            DurabilityLevel::MAJORITY,
            Duration::from_millis(0),
            &[0x01, 0x00, 0x01],
        );
    }

    #[test]
    fn test_durability_ext_frame_majority_duration_1() {
        test_one_durability(
            DurabilityLevel::MAJORITY,
            Duration::from_millis(1),
            &[0x01, 0x00, 0x01],
        );
    }

    #[test]
    fn test_durability_ext_frame_majority_duration_12201() {
        test_one_durability(
            DurabilityLevel::MAJORITY,
            Duration::from_millis(12201),
            &[0x01, 0x2f, 0xa9],
        );
    }

    #[test]
    fn test_durability_ext_frame_majority_duration_max() {
        test_one_durability(
            DurabilityLevel::MAJORITY,
            Duration::from_millis(65535),
            &[0x01, 0xff, 0xff],
        );
    }

    #[test]
    fn test_append_preserve_expiry() {
        let mut buf = [0; 128];
        let mut offset = 0;
        append_ext_frame(ExtReqFrameCode::PreserveTTL, &[], &mut buf, &mut offset).unwrap();

        assert_eq!(&buf[..offset], &[80]);
    }

    #[test]
    fn test_append_durability_level_no_timeout() {
        let mut buf = [0; 128];
        let mut offset = 0;
        append_ext_frame(ExtReqFrameCode::Durability, &[0x01], &mut buf, &mut offset).unwrap();

        assert_eq!(&buf[..offset], &[17, 1]);
    }

    #[test]
    fn test_append_durability_level_timeout() {
        let mut buf = [0u8; 128];
        let mut offset = 0;
        append_ext_frame(
            ExtReqFrameCode::Durability,
            &[0x01, 0x00, 0x01],
            &mut buf,
            &mut offset,
        )
        .unwrap();

        assert_eq!(&buf[..offset], &[19, 1, 0, 1]);
    }
}
