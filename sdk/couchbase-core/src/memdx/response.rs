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

use std::io::{Cursor, Read};
use std::time::Duration;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::error::{
    Error, ResourceError, ServerError, ServerErrorKind, SubdocError, SubdocErrorKind,
};
use crate::memdx::extframe::decode_res_ext_frames;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::OpsCrud;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::status::Status;
use crate::memdx::subdoc::SubDocResult;
use byteorder::{BigEndian, ReadBytesExt};
use tokio_io::Buf;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloResponse {
    pub enabled_features: Vec<HelloFeature>,
}

impl HelloResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        let mut features: Vec<HelloFeature> = Vec::new();
        if let Some(value) = &resp.value {
            if value.len() % 2 != 0 {
                return Err(Error::new_protocol_error("invalid hello features length"));
            }

            let mut cursor = Cursor::new(value);
            while let Ok(code) = cursor.read_u16::<BigEndian>() {
                features.push(HelloFeature::from(code));
            }
        }
        let response = HelloResponse {
            enabled_features: features,
        };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetErrorMapResponse {
    pub error_map: Vec<u8>,
}

impl GetErrorMapResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        let value = resp.value.unwrap_or_default();
        let response = GetErrorMapResponse { error_map: value };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketResponse {}

impl SelectBucketResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            if status == Status::AccessError || status == Status::KeyNotFound {
                return Err(ServerError::new(
                    ServerErrorKind::UnknownBucketName,
                    resp.op_code,
                    status,
                    resp.opaque,
                )
                .into());
            }
            return Err(OpsCore::decode_error(&resp));
        }

        Ok(SelectBucketResponse {})
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl SASLAuthResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status == Status::AuthContinue {
            return Ok(SASLAuthResponse {
                needs_more_steps: true,
                payload: resp.value.unwrap_or_default(),
            });
        }

        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        Ok(SASLAuthResponse {
            needs_more_steps: false,
            payload: resp.value.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLStepResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl SASLStepResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        Ok(SASLStepResponse {
            needs_more_steps: false,
            payload: resp.value.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsResponse {
    pub available_mechs: Vec<AuthMechanism>,
}

impl SASLListMechsResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            if status == Status::KeyNotFound {
                // KeyNotFound appears here when the bucket was initialized by ns_server, but
                // ns_server has not posted a configuration for the bucket to kv_engine yet. We
                // transform this into a ErrTmpFail as we make the assumption that the
                // SelectBucket will have failed if this was anything but a transient issue.
                return Err(ServerError::new(
                    ServerErrorKind::ConfigNotSet,
                    resp.op_code,
                    status,
                    resp.opaque,
                )
                .into());
            }
            return Err(OpsCore::decode_error(&resp));
        }

        let value = resp.value.unwrap_or_default();
        let mechs_list_string = match String::from_utf8(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::new_protocol_error(
                    "failed to parse authentication mechanism list",
                )
                .with(e));
            }
        };
        let mechs_list_split = mechs_list_string.split(' ');
        let mut mechs_list = Vec::new();
        for item in mechs_list_split {
            mechs_list.push(AuthMechanism::try_from(item)?);
        }

        Ok(SASLListMechsResponse {
            available_mechs: mechs_list,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetClusterConfigResponse {
    pub config: Vec<u8>,
}

impl GetClusterConfigResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        Ok(GetClusterConfigResponse {
            config: resp.value.clone().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BootstrapResult {
    pub hello: Option<HelloResponse>,
    pub error_map: Option<GetErrorMapResponse>,
    pub cluster_config: Option<GetClusterConfigResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub vbuuid: u64,
    pub seqno: u64,
}

impl TryFrom<&Vec<u8>> for MutationToken {
    type Error = Error;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() != 16 {
            return Err(Error::new_protocol_error("bad extras length"));
        }

        let (vbuuid_bytes, seqno_bytes) = value.split_at(size_of::<u64>());
        let vbuuid = u64::from_be_bytes(vbuuid_bytes.try_into().unwrap());
        let seqno = u64::from_be_bytes(seqno_bytes.try_into().unwrap());

        Ok(MutationToken { vbuuid, seqno })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SetResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl SetResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(SetResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

fn parse_flags(extras: &Option<Vec<u8>>) -> Result<u32, Error> {
    if let Some(extras) = &extras {
        if extras.len() != 4 {
            return Err(Error::new_protocol_error("bad extras length reading flags"));
        }

        Ok(u32::from_be_bytes(extras.as_slice().try_into().unwrap()))
    } else {
        Err(Error::new_protocol_error("no extras in response"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl GetResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let flags = parse_flags(&resp.extras)?;

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = resp.value.unwrap_or_default();

        Ok(GetResponse {
            cas: resp.cas.unwrap_or_default(),
            flags,
            value,
            datatype: resp.datatype,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetMetaResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
    pub expiry: u32,
    pub seq_no: u64,
    pub deleted: bool,
}

impl GetMetaResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = resp.value.unwrap_or_default();

        if let Some(extras) = &resp.extras {
            if extras.len() != 21 {
                return Err(Error::new_protocol_error("bad extras length"));
            }

            let mut extras = Cursor::new(extras);
            let deleted = extras.read_u32::<BigEndian>()?;
            let flags = extras.read_u32::<BigEndian>()?;
            let expiry = extras.read_u32::<BigEndian>()?;
            let seq_no = extras.read_u64::<BigEndian>()?;
            let datatype = extras.read_u8()?;

            Ok(GetMetaResponse {
                cas: resp.cas.unwrap_or_default(),
                flags,
                value,
                datatype,
                server_duration,
                expiry,
                seq_no,
                deleted: deleted != 0,
            })
        } else {
            Err(Error::new_protocol_error("no extras in response"))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DeleteResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl DeleteResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DeleteResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndLockResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl GetAndLockResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let flags = parse_flags(&resp.extras)?;

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = resp.value.unwrap_or_default();

        Ok(GetAndLockResponse {
            cas: resp.cas.unwrap_or_default(),
            flags,
            value,
            datatype: resp.datatype,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndTouchResponse {
    pub cas: u64,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub server_duration: Option<Duration>,
}

impl GetAndTouchResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let flags = parse_flags(&resp.extras)?;

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        let value = resp.value.unwrap_or_default();

        Ok(GetAndTouchResponse {
            cas: resp.cas.unwrap_or_default(),
            flags,
            value,
            datatype: resp.datatype,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnlockResponse {
    pub server_duration: Option<Duration>,
}

impl UnlockResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::NotLocked {
            return Err(ServerError::new(
                ServerErrorKind::NotLocked,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(UnlockResponse { server_duration })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TouchResponse {
    pub cas: u64,
    pub server_duration: Option<Duration>,
}

impl TouchResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        if let Some(extras) = &resp.extras {
            if !extras.is_empty() {
                return Err(Error::new_protocol_error("bad extras length"));
            }
        }

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(TouchResponse {
            cas: resp.cas.unwrap_or_default(),
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AddResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl AddResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::KeyExists)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AddResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReplaceResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl ReplaceResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(ReplaceResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AppendResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl AppendResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::NotStored {
            Some(ServerErrorKind::NotStored)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AppendResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PrependResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl PrependResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::NotStored {
            Some(ServerErrorKind::NotStored)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(PrependResponse {
            cas: resp.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IncrementResponse {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl IncrementResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::BadDelta {
            Some(ServerErrorKind::BadDelta)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let value = if let Some(val) = &resp.value {
            if val.len() != 8 {
                return Err(Error::new_protocol_error(
                    "bad counter value length in response",
                ));
            }

            u64::from_be_bytes(val.as_slice().try_into().unwrap())
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(IncrementResponse {
            cas: resp.cas.unwrap_or_default(),
            value,
            mutation_token,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DecrementResponse {
    pub cas: u64,
    pub value: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl DecrementResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::BadDelta {
            Some(ServerErrorKind::BadDelta)
        } else if status == Status::Success {
            None
        } else {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let value = if let Some(val) = &resp.value {
            if val.len() != 8 {
                return Err(Error::new_protocol_error(
                    "bad counter value length in response",
                ));
            }

            u64::from_be_bytes(val.as_slice().try_into().unwrap())
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &resp.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DecrementResponse {
            cas: resp.cas.unwrap_or_default(),
            value,
            mutation_token,
            server_duration,
        })
    }
}

pub struct LookupInResponse {
    pub cas: u64,
    pub ops: Vec<SubDocResult>,
    pub doc_is_deleted: bool,
    pub server_duration: Option<Duration>,
}

impl LookupInResponse {
    pub fn new(resp: ResponsePacket, op_count: usize) -> Result<Self, Error> {
        let cas = resp.cas;
        let status = resp.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::SubDocInvalidCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::InvalidCombo, None),
                },
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::SubDocInvalidXattrOrder {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None),
                },
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::SubDocXattrInvalidKeyCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None),
                },
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc {
                    error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None),
                },
                resp.op_code,
                resp.status,
                resp.opaque,
            )
            .into());
        }

        let mut doc_is_deleted = false;

        if status == Status::SubDocSuccessDeleted || status == Status::SubDocMultiPathFailureDeleted
        {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_error(&resp));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(op_count);
        let mut op_index = 0;

        let value = resp
            .value
            .as_ref()
            .ok_or_else(|| Error::new_protocol_error("missing value"))?;

        let mut cursor = Cursor::new(value);
        while cursor.position() < cursor.get_ref().len() as u64 {
            if cursor.remaining() < 6 {
                return Err(Error::new_protocol_error("bad value length"));
            }

            let res_status = cursor.read_u16::<BigEndian>()?;
            let res_status = Status::from(res_status);
            let res_value_len = cursor.read_u32::<BigEndian>()?;

            if cursor.remaining() < res_value_len as usize {
                return Err(Error::new_protocol_error("bad value length"));
            }

            let value = if res_value_len > 0 {
                let mut tmp_val = vec![0; res_value_len as usize];
                cursor.read_exact(&mut tmp_val)?;
                Some(tmp_val)
            } else {
                None
            };

            let err_kind: Option<SubdocErrorKind> = match res_status {
                Status::Success => None,
                Status::SubDocDocTooDeep => Some(SubdocErrorKind::DocTooDeep),
                Status::SubDocNotJSON => Some(SubdocErrorKind::NotJSON),
                Status::SubDocPathNotFound => Some(SubdocErrorKind::PathNotFound),
                Status::SubDocPathMismatch => Some(SubdocErrorKind::PathMismatch),
                Status::SubDocPathInvalid => Some(SubdocErrorKind::PathInvalid),
                Status::SubDocPathTooBig => Some(SubdocErrorKind::PathTooBig),
                Status::SubDocXattrUnknownVAttr => Some(SubdocErrorKind::XattrUnknownVAttr),
                _ => Some(SubdocErrorKind::UnknownStatus { status: res_status }),
            };

            let err = err_kind.map(|kind| {
                ServerError::new(
                    ServerErrorKind::Subdoc {
                        error: SubdocError::new(kind, op_index),
                    },
                    resp.op_code,
                    resp.status,
                    resp.opaque,
                )
                .into()
            });

            results.push(SubDocResult { value, err });
            op_index += 1;
        }

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(LookupInResponse {
            cas: cas.unwrap_or_default(),
            ops: results,
            doc_is_deleted,
            server_duration,
        })
    }
}

pub struct MutateInResponse {
    pub cas: u64,
    pub ops: Vec<SubDocResult>,
    pub doc_is_deleted: bool,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl MutateInResponse {
    pub fn new(resp: ResponsePacket, is_insert: bool, op_count: usize) -> Result<Self, Error> {
        let cas = resp.cas;
        let status = resp.status;

        let kind = if status == Status::KeyNotFound {
            Some(ServerErrorKind::KeyNotFound)
        } else if status == Status::KeyExists && cas.is_some() {
            Some(ServerErrorKind::CasMismatch)
        } else if status == Status::KeyExists {
            Some(ServerErrorKind::KeyExists)
        } else if status == Status::Locked {
            Some(ServerErrorKind::Locked)
        } else if status == Status::TooBig {
            Some(ServerErrorKind::TooBig)
        } else if status == Status::SubDocInvalidCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::InvalidCombo, None),
            })
        } else if status == Status::SubDocInvalidXattrOrder {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None),
            })
        } else if status == Status::SubDocXattrInvalidKeyCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None),
            })
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None),
            })
        } else if status == Status::SubDocXattrUnknownMacro {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrUnknownMacro, None),
            })
        } else if status == Status::SubDocXattrUnknownVattrMacro {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrUnknownVattrMacro, None),
            })
        } else if status == Status::SubDocXattrCannotModifyVAttr {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::XattrCannotModifyVAttr, None),
            })
        } else if status == Status::SubDocCanOnlyReviveDeletedDocuments {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::CanOnlyReviveDeletedDocuments, None),
            })
        } else if status == Status::SubDocDeletedDocumentCantHaveValue {
            Some(ServerErrorKind::Subdoc {
                error: SubdocError::new(SubdocErrorKind::DeletedDocumentCantHaveValue, None),
            })
        } else if status == Status::NotStored {
            if is_insert {
                Some(ServerErrorKind::KeyExists)
            } else {
                Some(ServerErrorKind::NotStored)
            }
        } else if status == Status::SubDocMultiPathFailure {
            if let Some(value) = &resp.value {
                if value.len() != 3 {
                    return Err(Error::new_protocol_error("bad value length"));
                }

                let mut cursor = Cursor::new(value);
                let op_index = cursor.read_u8()?;
                let res_status = cursor.read_u16::<BigEndian>()?;

                let res_status = Status::from(res_status);

                let err_kind: SubdocErrorKind = match res_status {
                    Status::SubDocDocTooDeep => SubdocErrorKind::DocTooDeep,
                    Status::SubDocNotJSON => SubdocErrorKind::NotJSON,
                    Status::SubDocPathNotFound => SubdocErrorKind::PathNotFound,
                    Status::SubDocPathMismatch => SubdocErrorKind::PathMismatch,
                    Status::SubDocPathInvalid => SubdocErrorKind::PathInvalid,
                    Status::SubDocPathTooBig => SubdocErrorKind::PathTooBig,
                    Status::SubDocPathExists => SubdocErrorKind::PathExists,
                    Status::SubDocCantInsert => SubdocErrorKind::CantInsert,
                    Status::SubDocBadRange => SubdocErrorKind::BadRange,
                    Status::SubDocBadDelta => SubdocErrorKind::BadDelta,
                    Status::SubDocValueTooDeep => SubdocErrorKind::ValueTooDeep,
                    _ => SubdocErrorKind::UnknownStatus { status: res_status },
                };

                Some(ServerErrorKind::Subdoc {
                    error: SubdocError::new(err_kind, Some(op_index)),
                })
            } else {
                return Err(Error::new_protocol_error("bad value length"));
            }
        } else {
            None
        };

        if let Some(kind) = kind {
            return Err(ServerError::new(kind, resp.op_code, status, resp.opaque).into());
        }

        let mut doc_is_deleted = false;
        if status == Status::SubDocSuccessDeleted {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_mutation_error(&resp));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(op_count);

        if let Some(value) = &resp.value {
            let mut cursor = Cursor::new(value);

            while cursor.position() < cursor.get_ref().len() as u64 {
                if cursor.remaining() < 3 {
                    return Err(Error::new_protocol_error("bad value length"));
                }

                let op_index = cursor.read_u8()?;

                if op_index > results.len() as u8 {
                    for _ in results.len() as u8..op_index {
                        results.push(SubDocResult {
                            err: None,
                            value: None,
                        });
                    }
                }

                let op_status = cursor.read_u16::<BigEndian>()?;
                let op_status = Status::from(op_status);

                if op_status == Status::Success {
                    let val_length = cursor.read_u32::<BigEndian>()?;

                    let mut value = vec![0; val_length as usize];
                    cursor.read_exact(&mut value)?;

                    results.push(SubDocResult {
                        err: None,
                        value: Some(value),
                    });
                } else {
                    return Err(Error::new_protocol_error(
                        "subdoc mutatein op illegally provided an error",
                    ));
                }
            }
        }

        if results.len() < op_count {
            for _ in results.len()..op_count {
                results.push(SubDocResult {
                    err: None,
                    value: None,
                });
            }
        }

        let mutation_token = if let Some(extras) = &resp.extras {
            if extras.len() != 16 {
                return Err(Error::new_protocol_error("bad extras length"));
            }

            let (vbuuid_bytes, seqno_bytes) = extras.split_at(size_of::<u64>());
            let vbuuid = u64::from_be_bytes(vbuuid_bytes.try_into().unwrap());
            let seqno = u64::from_be_bytes(seqno_bytes.try_into().unwrap());

            Some(MutationToken { vbuuid, seqno })
        } else {
            None
        };

        let server_duration = if let Some(f) = &resp.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(MutateInResponse {
            cas: cas.unwrap_or_default(),
            ops: results,
            mutation_token,
            doc_is_deleted,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetCollectionIdResponse {
    pub manifest_rev: u64,
    pub collection_id: u32,
}

impl GetCollectionIdResponse {
    pub fn new(
        resp: ResponsePacket,
        scope_name: &str,
        collection_name: &str,
    ) -> Result<Self, Error> {
        let status = resp.status;

        if status == Status::ScopeUnknown {
            return Err(ResourceError::new(
                ServerError::new(
                    ServerErrorKind::UnknownScopeName,
                    resp.op_code,
                    resp.status,
                    resp.opaque,
                ),
                scope_name,
                collection_name,
            )
            .into());
        } else if status == Status::CollectionUnknown {
            return Err(ResourceError::new(
                ServerError::new(
                    ServerErrorKind::UnknownCollectionName,
                    resp.op_code,
                    resp.status,
                    resp.opaque,
                ),
                scope_name,
                collection_name,
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        let extras = if let Some(extras) = &resp.extras {
            if extras.len() != 12 {
                return Err(Error::new_protocol_error("invalid extras length"));
            }
            extras
        } else {
            return Err(Error::new_protocol_error("no extras in response"));
        };

        let mut extras = Cursor::new(extras);
        let manifest_rev = extras.read_u64::<BigEndian>()?;
        let collection_id = extras.read_u32::<BigEndian>()?;

        Ok(GetCollectionIdResponse {
            manifest_rev,
            collection_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PingResponse {}

impl PingResponse {
    pub fn new(resp: ResponsePacket) -> Result<Self, Error> {
        let status = resp.status;

        if status != Status::Success {
            return Err(OpsCore::decode_error(&resp));
        }

        Ok(PingResponse {})
    }
}
