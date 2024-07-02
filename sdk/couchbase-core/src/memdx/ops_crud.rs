use std::time::Duration;

use byteorder::{BigEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};

use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::durability_level::{DurabilityLevel, DurabilityLevelSettings};
use crate::memdx::error::Error;
use crate::memdx::ext_frame_code::{ExtReqFrameCode, ExtResFrameCode};
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{GetRequest, SetRequest};
use crate::memdx::response::{GetResponse, SetResponse};
use crate::memdx::status::Status;

#[derive(Debug)]
pub struct OpsCrud {
    pub collections_enabled: bool,
    pub durability_enabled: bool,
    pub preserve_expiry_enabled: bool,
    pub ext_frames_enabled: bool,
}

impl OpsCrud {
    pub async fn set<D>(
        &self,
        dispatcher: &mut D,
        request: SetRequest,
    ) -> Result<StandardPendingOp<SetResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            request.preserve_expiry,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let mut extra_buf: Vec<u8> = Vec::with_capacity(8);
        extra_buf.write_u32::<BigEndian>(request.flags)?;
        extra_buf.write_u32::<BigEndian>(request.expiry.unwrap_or_default())?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Set,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(request.value),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(pending_op))
    }
    pub async fn get<D>(
        &self,
        dispatcher: &mut D,
        request: GetRequest,
    ) -> Result<StandardPendingOp<GetResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic =
            self.encode_req_ext_frames(None, None, None, request.on_behalf_of, &mut ext_frame_buf)?;

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Get,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    fn encode_collection_and_key(&self, collection_id: u32, key: Vec<u8>) -> Result<Vec<u8>> {
        if !self.collections_enabled {
            if collection_id != 0 {
                return Err(Error::CollectionsNotEnabled);
            }

            return Ok(key);
        }

        Ok(make_uleb128_32(key, collection_id))
    }

    fn encode_req_ext_frames(
        &self,
        durability_level: Option<DurabilityLevel>,
        durability_timeout: Option<Duration>,
        preserve_expiry: Option<bool>,
        on_behalf_of: Option<String>,
        buf: &mut Vec<u8>,
    ) -> Result<Magic> {
        if let Some(obo) = on_behalf_of {
            append_ext_frame(ExtReqFrameCode::OnBehalfOf, obo.into_bytes(), buf)?;
        }

        if let Some(dura) = durability_level {
            if !self.durability_enabled {
                return Err(Error::Protocol(
                    "Cannot use synchronous durability when its not enabled".to_string(),
                ));
            }

            let dura_buf = encode_durability_ext_frame(dura, durability_timeout)?;

            append_ext_frame(ExtReqFrameCode::Durability, dura_buf, buf)?;
        } else if durability_timeout.is_some() {
            return Err(Error::Protocol(
                "Cannot encode durability timeout without durability level".to_string(),
            ));
        }

        if preserve_expiry.is_some() {
            if !self.preserve_expiry_enabled {
                return Err(Error::Protocol(
                    "Cannot use preserve expiry when its not enabled".to_string(),
                ));
            }

            append_ext_frame(ExtReqFrameCode::PreserveTTL, vec![], buf)?;
        }

        let magic = if !buf.is_empty() {
            if !self.ext_frames_enabled {
                return Err(Error::Protocol(
                    "Cannot use framing extras when its not enabled".to_string(),
                ));
            }

            Magic::ReqExt
        } else {
            Magic::Req
        };

        Ok(magic)
    }

    pub(crate) fn decode_common_status(status: Status) -> Result<()> {
        let err = match status {
            Status::CollectionUnknown => Error::UnknownCollectionID,
            Status::AccessError => Error::Access,
            _ => {
                return Ok(());
            }
        };

        Err(err)
    }

    pub(crate) fn decode_common_error(resp: &ResponsePacket) -> Error {
        if let Err(e) = Self::decode_common_status(resp.status) {
            return e;
        };

        OpsCore::decode_error(resp)
    }
}

pub(crate) fn decode_res_ext_frames(buf: &[u8]) -> Result<Option<Duration>> {
    let mut server_duration_data = None;

    iter_ext_frames(buf, |code, data| {
        if code == ExtResFrameCode::ServerDuration {
            server_duration_data = Some(data);
        }
    })?;

    if let Some(data) = server_duration_data {
        return Ok(Some(decode_server_duration_ext_frame(data)?));
    }

    Ok(None)
}

pub fn decode_ext_frame(buf: &[u8]) -> Result<(ExtResFrameCode, Vec<u8>, usize)> {
    if buf.is_empty() {
        return Err(Error::Protocol("Framing extras protocol error".to_string()));
    }

    let mut buf_pos = 0;

    let frame_header = buf[buf_pos];
    let mut u_frame_code = (frame_header & 0xF0) >> 4;
    let mut frame_code = ExtResFrameCode::from(u_frame_code as u16);
    let mut frame_len = frame_header & 0x0F;
    buf_pos += 1;

    if u_frame_code == 15 {
        if buf.len() < buf_pos + 1 {
            return Err(Error::Protocol("Unexpected eof".to_string()));
        }

        let frame_code_ext = buf[buf_pos];
        u_frame_code = 15 + frame_code_ext;
        frame_code = ExtResFrameCode::from(u_frame_code as u16);
        buf_pos += 1;
    }

    if frame_len == 15 {
        if buf.len() < buf_pos + 1 {
            return Err(Error::Protocol("Unexpected eof".to_string()));
        }

        let frame_len_ext = buf[buf_pos];
        frame_len = 15 + frame_len_ext;
        buf_pos += 1;
    }

    let u_frame_len = frame_len as usize;
    if buf.len() < buf_pos + u_frame_len {
        return Err(Error::Protocol("unexpected eof".to_string()));
    }

    let frame_body = &buf[buf_pos..buf_pos + u_frame_len];
    buf_pos += u_frame_len;

    Ok((frame_code, frame_body.to_vec(), buf_pos))
}

fn iter_ext_frames(buf: &[u8], mut cb: impl FnMut(ExtResFrameCode, Vec<u8>)) -> Result<Vec<u8>> {
    if !buf.is_empty() {
        let (frame_code, frame_body, buf_pos) = decode_ext_frame(buf)?;

        cb(frame_code, frame_body);

        return Ok(buf[buf_pos..].to_vec());
    }

    Ok(Vec::from(buf))
}

pub fn append_ext_frame(
    frame_code: ExtReqFrameCode,
    frame_body: Vec<u8>,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let frame_len = frame_body.len();
    let buf_len = buf.len();

    buf.push(0);
    let hdr_byte_ptr = &mut buf[buf_len - 1];
    let u_frame_code: u16 = frame_code.into();

    if u_frame_code < 15 {
        *hdr_byte_ptr = (*hdr_byte_ptr as u16 | ((u_frame_code & 0x0f) << 4)) as u8;
    } else {
        if u_frame_code - 15 >= 15 {
            return Err(Error::Protocol(
                "Extframe code too large to encode".to_string(),
            ));
        }

        *hdr_byte_ptr |= 0xF0;
        buf.put_u16(u_frame_code);
    }

    if frame_len > 0 {
        buf.extend_from_slice(&frame_body);
    }

    Ok(())
}

pub fn make_uleb128_32(key: Vec<u8>, collection_id: u32) -> Vec<u8> {
    let mut cid = collection_id;
    let mut builder = BytesMut::with_capacity(key.len() + 5);
    loop {
        let mut c: u8 = (cid & 0x7f) as u8;
        cid >>= 7;
        if cid != 0 {
            c |= 0x80;
        }

        builder.put_u8(c);
        if c & 0x80 == 0 {
            break;
        }
    }
    for k in key {
        builder.put_u8(k);
    }

    builder.freeze().to_vec()
}

fn encode_durability_ext_frame(
    level: DurabilityLevel,
    timeout: Option<Duration>,
) -> Result<Vec<u8>> {
    if timeout.is_none() {
        return Ok(vec![level.into()]);
    }

    let timeout = timeout.unwrap();

    let mut timeout_millis = timeout.as_millis();
    if timeout_millis > 65535 {
        return Err(Error::Protocol(
            "Cannot encode durability timeout greater than 65535 milliseconds".to_string(),
        ));
    }

    if timeout_millis == 0 {
        timeout_millis = 1;
    }

    let mut buf = vec![level.into()];
    buf.put_u128(timeout_millis >> 8);
    buf.put_u128(timeout_millis);

    Ok(buf)
}

pub(crate) fn decode_server_duration_ext_frame(mut data: Vec<u8>) -> Result<Duration> {
    if data.len() != 2 {
        return Err(Error::Protocol(
            "Invalid server duration extframe length".to_string(),
        ));
    }

    let dura_enc = (data.remove(0) as u32) << 8 | (data.remove(0) as u32);
    let dura_micros = ((dura_enc as f32).powf(1.74) / 2.0).round();

    Ok(Duration::from_micros(dura_micros as u64))
}

pub(crate) fn decode_durability_level_ext_frame(
    data: &mut Vec<u8>,
) -> Result<DurabilityLevelSettings> {
    if data.len() == 1 {
        let durability = DurabilityLevel::from(data.remove(0));

        return Ok(DurabilityLevelSettings::new(durability));
    } else if data.len() == 3 {
        let durability = DurabilityLevel::from(data.remove(0));
        let timeout_millis = (data.remove(0) as u32) << 8 | (data.remove(0) as u32);

        return Ok(DurabilityLevelSettings::new_with_timeout(
            durability,
            Duration::from_millis(timeout_millis as u64),
        ));
    }

    Err(Error::Protocol(
        "Invalid durability extframe length".to_string(),
    ))
}
