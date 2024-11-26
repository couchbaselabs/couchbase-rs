use std::time::Duration;
use byteorder::{BigEndian, WriteBytesExt, ByteOrder};
use bytes::{BufMut, BytesMut};
use crate::memdx::client::ResponseContext;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::durability_level::{DurabilityLevel, DurabilityLevelSettings};
use crate::memdx::error::Result;
use crate::memdx::error::{Error, ServerError, ServerErrorKind};
use crate::memdx::ext_frame_code::{ExtReqFrameCode, ExtResFrameCode};
use crate::memdx::magic::Magic;
use crate::memdx::opcode::OpCode;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::packet::{RequestPacket, ResponsePacket};
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{AddRequest, AppendRequest, DecrementRequest, DeleteRequest, GetAndLockRequest, GetAndTouchRequest, GetMetaRequest, GetRequest, IncrementRequest, LookupInRequest, MutateInRequest, PrependRequest, ReplaceRequest, SetRequest, TouchRequest, UnlockRequest};
use crate::memdx::response::{AddResponse, AppendResponse, DecrementResponse, DeleteResponse, GetAndLockResponse, GetAndTouchResponse, GetMetaResponse, GetResponse, IncrementResponse, LookupInResponse, MutateInResponse, PrependResponse, ReplaceResponse, SetResponse, TouchResponse, UnlockResponse};
use crate::memdx::status::Status;
use crate::memdx::subdoc::SubdocRequestInfo;

#[derive(Debug)]
pub struct OpsCrud {
    pub collections_enabled: bool,
    pub durability_enabled: bool,
    pub preserve_expiry_enabled: bool,
    pub ext_frames_enabled: bool,
}

impl OpsCrud {
    pub async fn set<'a, D>(
        &self,
        dispatcher: &D,
        request: SetRequest<'a>,
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
        extra_buf
            .write_u32::<BigEndian>(request.flags)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request flags",
                    "flags",
                    Box::new(e),
                )
            })?;
        extra_buf
            .write_u32::<BigEndian>(request.expiry.unwrap_or_default())
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Set,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(request.value.to_vec()),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get<'a, D>(
        &self,
        dispatcher: &D,
        request: GetRequest<'a>,
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

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_meta<'a, D>(
        &self,
        dispatcher: &D,
        request: GetMetaRequest<'a>,
    ) -> Result<StandardPendingOp<GetMetaResponse>>
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

        // This appears to be necessary to get the server to include the datatype in the response
        // extras.
        let extras = [2];

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GetMeta,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extras.to_vec()),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn delete<'a, D>(
        &self,
        dispatcher: &D,
        request: DeleteRequest<'a>,
    ) -> Result<StandardPendingOp<DeleteResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Delete,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_and_lock<'a, D>(
        &self,
        dispatcher: &D,
        request: GetAndLockRequest<'a>,
    ) -> Result<StandardPendingOp<GetAndLockResponse>>
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

        let mut extra_buf: Vec<u8> = Vec::with_capacity(4);
        extra_buf
            .write_u32::<BigEndian>(request.lock_time)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request lock time",
                    "lock_time",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GetLocked,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn get_and_touch<'a, D>(
        &self,
        dispatcher: &D,
        request: GetAndTouchRequest<'a>,
    ) -> Result<StandardPendingOp<GetAndTouchResponse>>
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

        let mut extra_buf: Vec<u8> = Vec::with_capacity(4);
        extra_buf
            .write_u32::<BigEndian>(request.expiry)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::GAT,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn unlock<'a, D>(
        &self,
        dispatcher: &D,
        request: UnlockRequest<'a>,
    ) -> Result<StandardPendingOp<UnlockResponse>>
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
            op_code: OpCode::UnlockKey,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: Some(request.cas),
            extras: None,
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn touch<'a, D>(
        &self,
        dispatcher: &D,
        request: TouchRequest<'a>,
    ) -> Result<StandardPendingOp<TouchResponse>>
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

        let mut extra_buf: Vec<u8> = Vec::with_capacity(4);
        extra_buf
            .write_u32::<BigEndian>(request.expiry)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Touch,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn add<'a, D>(
        &self,
        dispatcher: &D,
        request: AddRequest<'a>,
    ) -> Result<StandardPendingOp<AddResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
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
        extra_buf
            .write_u32::<BigEndian>(request.flags)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request flags",
                    "flags",
                    Box::new(e),
                )
            })?;
        extra_buf
            .write_u32::<BigEndian>(request.expiry.unwrap_or_default())
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Add,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(request.value.to_vec()),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn replace<'a, D>(
        &self,
        dispatcher: &D,
        request: ReplaceRequest<'a>,
    ) -> Result<StandardPendingOp<ReplaceResponse>>
    where
        D: Dispatcher,
    {
        if request.expiry.is_some() && request.preserve_expiry.is_some() {
            return Err(Error::protocol_error("Cannot specify expiry and preserve expiry"));
        }

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
        extra_buf
            .write_u32::<BigEndian>(request.flags)
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request flags",
                    "flags",
                    Box::new(e),
                )
            })?;
        extra_buf
            .write_u32::<BigEndian>(request.expiry.unwrap_or_default())
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Replace,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(request.value.to_vec()),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn append<'a, D>(
        &self,
        dispatcher: &D,
        request: AppendRequest<'a>,
    ) -> Result<StandardPendingOp<AppendResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Append,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: Some(request.value.to_vec()),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn prepend<'a, D>(
        &self,
        dispatcher: &D,
        request: PrependRequest<'a>,
    ) -> Result<StandardPendingOp<PrependResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Prepend,
            datatype: request.datatype,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: None,
            key: Some(key),
            value: Some(request.value.to_vec()),
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    fn encode_counter_values(
        delta: Option<u64>,
        initial: Option<u64>,
        expiry: Option<u32>,
    ) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(20);
        if initial.unwrap_or_default() != 0xFFFFFFFFFFFFFFFF {
            buf.write_u64::<BigEndian>(delta.unwrap_or_default())
                .map_err(|e| {
                    Error::invalid_argument_error_with_source(
                        "failed to write request delta",
                        "delta",
                        Box::new(e),
                    )
                })?;

            buf.write_u64::<BigEndian>(initial.unwrap_or_default())
                .map_err(|e| {
                    Error::invalid_argument_error_with_source(
                        "failed to write request intial value",
                        "initial",
                        Box::new(e),
                    )
                })?;

            buf.write_u32::<BigEndian>(expiry.unwrap_or_default())
                .map_err(|e| {
                    Error::invalid_argument_error_with_source(
                        "failed to write request expiry",
                        "expiry",
                        Box::new(e),
                    )
                })?;
        } else {
            // These are dev time issues and should never occur.
            buf.write_u64::<BigEndian>(0x0000000000000000).unwrap();
            buf.write_u32::<BigEndian>(0xFFFFFFFF).unwrap();
        }

        Ok(buf)
    }

    pub async fn increment<'a, D>(
        &self,
        dispatcher: &D,
        request: IncrementRequest<'a>,
    ) -> Result<StandardPendingOp<IncrementResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let extra_buf =
            Self::encode_counter_values(request.delta, request.initial, request.expiry)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Increment,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn decrement<'a, D>(
        &self,
        dispatcher: &D,
        request: DecrementRequest<'a>,
    ) -> Result<StandardPendingOp<DecrementResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            request.durability_level,
            request.durability_level_timeout,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let extra_buf =
            Self::encode_counter_values(request.delta, request.initial, request.expiry)?;

        let packet = RequestPacket {
            magic,
            op_code: OpCode::Decrement,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: None,
            framing_extras,
            opaque: None,
        };

        let pending_op = dispatcher.dispatch(packet, None).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn lookup_in<'a, D>(
        &self,
        dispatcher: &D,
        request: LookupInRequest<'a>,
    ) -> Result<StandardPendingOp<LookupInResponse>>
    where
        D: Dispatcher,
    {
        let mut ext_frame_buf: Vec<u8> = vec![];
        let magic = self.encode_req_ext_frames(
            None,
            None,
            None,
            request.on_behalf_of,
            &mut ext_frame_buf,
        )?;

        let framing_extras = if !ext_frame_buf.is_empty() {
            Some(ext_frame_buf)
        } else {
            None
        };

        let key = self.encode_collection_and_key(request.collection_id, request.key)?;

        let len_ops = request.ops.len();
        let mut path_bytes_list: Vec<Vec<u8>> = vec![vec![]; len_ops];
        let mut path_bytes_total = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = op.path.to_vec();
            path_bytes_total += path_bytes.len();
            path_bytes_list[i] = path_bytes;
        }

        let mut value_buf: Vec<u8> = vec![0; len_ops * 4 + path_bytes_total];
        let mut value_iter = 0;
        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = &path_bytes_list[i];
            let path_bytes_len = path_bytes.len();

            value_buf[value_iter] = Into::<OpCode>::into(op.op).into();
            value_buf[value_iter + 1] = op.flags as u8;
            BigEndian::write_u16(&mut value_buf[value_iter + 2..value_iter + 4], path_bytes_len as u16);
            value_buf[value_iter + 4..value_iter + 4 + path_bytes_len].copy_from_slice(path_bytes);
            value_iter += 4 + path_bytes_len;
        }

        let mut extra_buf = Vec::with_capacity(8);

        if let Some(flags) = request.flags {
            extra_buf.push(flags as u8);
        }

        let packet = RequestPacket {
            magic,
            op_code: OpCode::SubDocMultiLookup,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: None,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(value_buf),
            framing_extras,
            opaque: None,
        };

        let response_context = ResponseContext {
            cas: packet.cas,
            subdoc_info: Some(SubdocRequestInfo {
                flags: request.flags,
                op_count: request.ops.len() as u8,
            })
        };

        let pending_op = dispatcher.dispatch(packet, Some(response_context)).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    pub async fn mutate_in<'a, D>(
        &self,
        dispatcher: &D,
        request: MutateInRequest<'a>,
    ) -> Result<StandardPendingOp<MutateInResponse>>
    where
        D: Dispatcher,
    {
        if request.expiry.is_some() && request.preserve_expiry.is_some() {
            return Err(Error::protocol_error("Cannot specify expiry and preserve expiry"));
        }

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

        let len_ops = request.ops.len();
        let mut path_bytes_list: Vec<Vec<u8>> = vec![vec![]; len_ops];
        let mut path_bytes_total = 0;
        let mut value_bytes_total = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = op.path.to_vec();
            path_bytes_total += path_bytes.len();
            path_bytes_list[i] = path_bytes;
            value_bytes_total += op.value.len();
        }

        let mut value_buf: Vec<u8> = vec![0; len_ops * 8 + path_bytes_total + value_bytes_total];
        let mut value_iter = 0;

        for (i, op) in request.ops.iter().enumerate() {
            let path_bytes = &path_bytes_list[i];
            let path_bytes_len = path_bytes.len();
            let value_bytes_len = op.value.len();

            value_buf[value_iter] = Into::<OpCode>::into(op.op).into();
            value_buf[value_iter + 1] = op.flags as u8;
            BigEndian::write_u16(&mut value_buf[value_iter + 2..value_iter + 4], path_bytes_len as u16);
            BigEndian::write_u32(&mut value_buf[value_iter + 4.. value_iter + 8], value_bytes_len as u32);
            value_buf[value_iter + 8..value_iter + 8 + path_bytes_len].copy_from_slice(path_bytes);
            value_buf[value_iter + 8 + path_bytes_len..value_iter + 8 + path_bytes_len + value_bytes_len].copy_from_slice(op.value);
            value_iter += 8 + path_bytes_len + value_bytes_len;
        }

        let mut extra_buf: Vec<u8> = Vec::with_capacity(5);

        extra_buf.write_u32::<BigEndian>(request.expiry.unwrap_or_default())
            .map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request expiry",
                    "expiry",
                    Box::new(e),
                )
            })?;

        if let Some(flags) = request.flags {
            extra_buf.write_u8(flags as u8).map_err(|e| {
                Error::invalid_argument_error_with_source(
                    "failed to write request flags",
                    "flags",
                    Box::new(e),
                )
            })?;
        }

        let packet = RequestPacket {
            magic,
            op_code: OpCode::SubDocMultiMutation,
            datatype: 0,
            vbucket_id: Some(request.vbucket_id),
            cas: request.cas,
            extras: Some(extra_buf),
            key: Some(key),
            value: Some(value_buf),
            framing_extras,
            opaque: None,
        };

        let response_context = ResponseContext {
            cas: request.cas,
            subdoc_info: Some(SubdocRequestInfo {
                flags: request.flags,
                op_count: request.ops.len() as u8,
            })
        };

        let pending_op = dispatcher.dispatch(packet, Some(response_context)).await?;

        Ok(StandardPendingOp::new(pending_op))
    }

    fn encode_collection_and_key(&self, collection_id: u32, key: &[u8]) -> Result<Vec<u8>> {
        if !self.collections_enabled {
            if collection_id != 0 {
                return Err(Error::invalid_argument_error(
                    "collections not enabled",
                    "collection_id",
                ));
            }

            return Ok(key.to_vec());
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
                return Err(Error::invalid_argument_error(
                    "Cannot use synchronous durability when its not enabled",
                    "durability_level",
                ));
            }

            let dura_buf = encode_durability_ext_frame(dura, durability_timeout)?;

            append_ext_frame(ExtReqFrameCode::Durability, dura_buf, buf)?;
        } else if durability_timeout.is_some() {
            return Err(Error::invalid_argument_error(
                "Cannot encode durability timeout without durability level",
                "durability_timeout",
            ));
        }

        if preserve_expiry.is_some() {
            if !self.preserve_expiry_enabled {
                return Err(Error::invalid_argument_error(
                    "Cannot use preserve expiry when its not enabled",
                    "preserve_expiry",
                ));
            }

            append_ext_frame(ExtReqFrameCode::PreserveTTL, vec![], buf)?;
        }

        let magic = if !buf.is_empty() {
            if !self.ext_frames_enabled {
                return Err(Error::invalid_argument_error(
                    "Cannot use framing extras when its not enabled",
                    "ext_frames_enabled",
                ));
            }

            Magic::ReqExt
        } else {
            Magic::Req
        };

        Ok(magic)
    }

    pub(crate) fn decode_common_status(resp: &ResponsePacket) -> std::result::Result<(), Error> {
        let kind = match resp.status {
            Status::CollectionUnknown => ServerErrorKind::UnknownCollectionID,
            Status::AccessError => ServerErrorKind::Access,
            Status::NoBucket => ServerErrorKind::NoBucket,
            _ => {
                return Ok(());
            }
        };

        Err(ServerError::new(kind, resp).into())
    }

    pub(crate) fn decode_common_error(resp: &ResponsePacket) -> Error {
        if let Err(e) = Self::decode_common_status(resp) {
            return e;
        };

        OpsCore::decode_error(resp).into()
    }
}

pub(crate) fn decode_res_ext_frames(buf: &[u8]) -> Result<Option<Duration>> {
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

pub fn decode_ext_frame(buf: &[u8]) -> Result<(ExtResFrameCode, &[u8], usize)> {
    if buf.is_empty() {
        return Err(Error::protocol_error(
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
            return Err(Error::protocol_error("unexpected eof decoding ext frame"));
        }

        let frame_code_ext = buf[buf_pos];
        u_frame_code = 15 + frame_code_ext;
        frame_code = ExtResFrameCode::from(u_frame_code as u16);
        buf_pos += 1;
    }

    if frame_len == 15 {
        if buf.len() < buf_pos + 1 {
            return Err(Error::protocol_error("unexpected eof decoding ext frame"));
        }

        let frame_len_ext = buf[buf_pos];
        frame_len = 15 + frame_len_ext;
        buf_pos += 1;
    }

    let u_frame_len = frame_len as usize;
    if buf.len() < buf_pos + u_frame_len {
        return Err(Error::protocol_error("unexpected eof decoding ext frame"));
    }

    let frame_body = &buf[buf_pos..buf_pos + u_frame_len];
    buf_pos += u_frame_len;

    Ok((frame_code, frame_body, buf_pos))
}

fn iter_ext_frames(buf: &[u8], mut cb: impl FnMut(ExtResFrameCode, &[u8])) -> Result<&[u8]> {
    if !buf.is_empty() {
        let (frame_code, frame_body, buf_pos) = decode_ext_frame(buf)?;

        cb(frame_code, frame_body);

        return Ok(&buf[buf_pos..]);
    }

    Ok(buf)
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
            return Err(Error::invalid_argument_error(
                "ext frame code too large to encode",
                "ext frame",
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

pub fn make_uleb128_32(key: &[u8], collection_id: u32) -> Vec<u8> {
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
        builder.put_u8(*k);
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
        return Err(Error::invalid_argument_error(
            "Cannot encode durability timeout greater than 65535 milliseconds",
            "durability_level_timeout",
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

pub(crate) fn decode_server_duration_ext_frame(mut data: &[u8]) -> Result<Duration> {
    if data.len() != 2 {
        return Err(Error::protocol_error(
            "invalid server duration ext frame length",
        ));
    }

    let dura_enc = (data[0] as u32) << 8 | (data[1] as u32);
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

    Err(Error::protocol_error("invalid durability ext frame length"))
}
