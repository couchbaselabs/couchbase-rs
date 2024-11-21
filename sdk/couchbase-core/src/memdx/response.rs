use std::io::{Cursor, Read};
use std::time::Duration;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::{Error, ErrorKind, ResourceError, ServerError, ServerErrorKind, SubdocError, SubdocErrorKind};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::{decode_res_ext_frames, OpsCrud};
use crate::memdx::status::Status;
use byteorder::{BigEndian, ReadBytesExt};
use tokio_io::Buf;
use crate::memdx::subdoc::{SubDocResult, SubdocDocFlag};

pub trait TryFromClientResponse: Sized {
    fn try_from(resp: ClientResponse) -> Result<Self, Error>;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloResponse {
    pub enabled_features: Vec<HelloFeature>,
}

impl TryFromClientResponse for HelloResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        let mut features: Vec<HelloFeature> = Vec::new();
        if let Some(value) = &packet.value {
            if value.len() % 2 != 0 {
                return Err(Error::protocol_error("invalid hello features length"));
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

impl TryFromClientResponse for GetErrorMapResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        // TODO: Clone?
        let value = packet.value.clone().unwrap_or_default();
        let response = GetErrorMapResponse { error_map: value };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketResponse {}

impl TryFromClientResponse for SelectBucketResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            if status == Status::AccessError || status == Status::KeyNotFound {
                return Err(ErrorKind::UnknownBucketName.into());
            }
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        Ok(SelectBucketResponse {})
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl TryFromClientResponse for SASLAuthResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status == Status::SASLAuthContinue {
            // TODO: clone?
            let value = packet.value.clone();
            return Ok(SASLAuthResponse {
                needs_more_steps: true,
                payload: value.unwrap_or_default(),
            });
        }

        if status != Status::Success {
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        Ok(SASLAuthResponse {
            needs_more_steps: false,
            // TODO: clone?
            payload: packet.value.clone().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLStepResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl TryFromClientResponse for SASLStepResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        Ok(SASLStepResponse {
            needs_more_steps: false,
            // TODO: clone?
            payload: packet.value.clone().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsResponse {
    pub available_mechs: Vec<AuthMechanism>,
}

impl TryFromClientResponse for SASLListMechsResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            if status == Status::KeyNotFound {
                // KeyNotFound appears here when the bucket was initialized by ns_server, but
                // ns_server has not posted a configuration for the bucket to kv_engine yet. We
                // transform this into a ErrTmpFail as we make the assumption that the
                // SelectBucket will have failed if this was anything but a transient issue.
                return Err(ServerError::new(
                    ServerErrorKind::ConfigNotSet,
                    packet,
                    resp.local_addr(),
                    resp.peer_addr(),
                )
                .into());
            }
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        // TODO: Clone?
        let value = packet.value.clone().unwrap_or_default();
        let mechs_list_string = match String::from_utf8(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::protocol_error_with_source(
                    "failed to parse authentication mechanism list",
                    Box::new(e),
                ));
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
    fn replace(search: &[u8], find: u8, replace: &[u8]) -> Vec<u8> {
        let mut result = vec![];

        for &b in search {
            if b == find {
                result.extend(replace);
            } else {
                result.push(b);
            }
        }

        result
    }
}

impl TryFromClientResponse for GetClusterConfigResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;
        if status != Status::Success {
            return Err(OpsCore::decode_error(packet, resp.local_addr(), resp.peer_addr()).into());
        }

        let host = match resp.local_addr() {
            Some(addr) => addr.ip().to_string(),
            None => {
                return Err(Error::protocol_error(
                    "Failed to identify memd hostname for $HOST replacement",
                ));
            }
        };

        // TODO: Clone, maybe also inefficient?
        let value = match std::str::from_utf8(packet.value.clone().unwrap_or_default().as_slice()) {
            Ok(v) => v.to_string(),
            Err(_e) => "".to_string(),
        };

        let out = value.replace("$HOST", host.as_ref());

        Ok(GetClusterConfigResponse {
            config: out.into_bytes(),
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
            return Err(Error::protocol_error("bad extras length"));
        }

        let mut extras = Cursor::new(value);

        Ok(MutationToken {
            vbuuid: extras.read_u64::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to parse vbuuid for mutation token",
                    Box::new(e),
                )
            })?,
            seqno: extras.read_u64::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to parse seqno for mutation token",
                    Box::new(e),
                )
            })?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SetResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for SetResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::KeyExists {
            return Err(ServerError::new(
                ServerErrorKind::KeyExists,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(SetResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
    }
}

fn parse_flags(extras: &Option<Vec<u8>>) -> Result<u32, Error> {
    if let Some(extras) = &extras {
        if extras.len() != 4 {
            return Err(Error::protocol_error("bad extras length reading flags"));
        }

        let mut extras = Cursor::new(extras);
        extras.read_u32::<BigEndian>().map_err(|e| {
            Error::protocol_error_with_source("failed to read flags from extras", Box::new(e))
        })
    } else {
        Err(Error::protocol_error("no extras in response"))
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

impl TryFromClientResponse for GetResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        // TODO: clone
        let value = packet.value.clone().unwrap_or_default();

        Ok(GetResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
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

impl TryFromClientResponse for GetMetaResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        // TODO: clone
        let value = packet.value.clone().unwrap_or_default();

        if let Some(extras) = &packet.extras {
            if extras.len() != 21 {
                return Err(Error::protocol_error("bad extras length"));
            }

            let mut extras = Cursor::new(extras);
            let deleted = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to parse deleted from extras",
                    Box::new(e),
                )
            })?;
            let flags = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source("failed to parse flags from extras", Box::new(e))
            })?;
            let expiry = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source("failed to parse expiry from extras", Box::new(e))
            })?;
            let seq_no = extras.read_u64::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source("failed to parse seq_no from extras", Box::new(e))
            })?;
            let datatype = extras.read_u8().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to parse datatype from extras",
                    Box::new(e),
                )
            })?;

            Ok(GetMetaResponse {
                cas: packet.cas.unwrap_or_default(),
                flags,
                value,
                datatype,
                server_duration,
                expiry,
                seq_no,
                deleted: deleted != 0,
            })
        } else {
            Err(Error::protocol_error("no extras in response"))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DeleteResponse {
    pub cas: u64,
    pub mutation_token: Option<MutationToken>,
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for DeleteResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::KeyExists {
            return Err(ServerError::new(
                ServerErrorKind::KeyExists,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DeleteResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for GetAndLockResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        // TODO: clone
        let value = packet.value.clone().unwrap_or_default();

        Ok(GetAndLockResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
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

impl TryFromClientResponse for GetAndTouchResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let flags = parse_flags(&packet.extras)?;

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        // TODO: clone
        let value = packet.value.clone().unwrap_or_default();

        Ok(GetAndTouchResponse {
            cas: packet.cas.unwrap_or_default(),
            flags,
            value,
            datatype: packet.datatype,
            server_duration,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnlockResponse {
    pub server_duration: Option<Duration>,
}

impl TryFromClientResponse for UnlockResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::NotLocked {
            return Err(ServerError::new(
                ServerErrorKind::NotLocked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
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

impl TryFromClientResponse for TouchResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        if let Some(extras) = &packet.extras {
            if !extras.is_empty() {
                return Err(Error::protocol_error("bad extras length"));
            }
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(TouchResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for AddResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::KeyExists {
            return Err(ServerError::new(
                ServerErrorKind::KeyExists,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AddResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for ReplaceResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::KeyExists {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(ReplaceResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for AppendResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        // KeyExists without a request cas would be an odd error to receive so we don't
        // handle that case.
        if status == Status::KeyExists && resp.response_context().cas.is_some() {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::NotStored {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(AppendResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for PrependResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        // KeyExists without a request cas would be an odd error to receive so we don't
        // handle that case.
        if status == Status::KeyExists && resp.response_context().cas.is_some() {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::NotStored {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(PrependResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for IncrementResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let value = if let Some(val) = &resp.packet().value {
            if val.len() != 8 {
                return Err(Error::protocol_error(
                    "bad counter value length in response",
                ));
            }
            let mut val = Cursor::new(val);

            val.read_u64::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to read counter value from response",
                    Box::new(e),
                )
            })?
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(IncrementResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for DecrementResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            )
            .into());
        } else if status != Status::Success {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let value = if let Some(val) = &resp.packet().value {
            if val.len() != 8 {
                return Err(Error::protocol_error(
                    "bad counter value length in response",
                ));
            }
            let mut val = Cursor::new(val);

            val.read_u64::<BigEndian>().map_err(|e| {
                Error::protocol_error_with_source(
                    "failed to read counter value from response",
                    Box::new(e),
                )
            })?
        } else {
            0
        };

        let mutation_token = if let Some(extras) = &packet.extras {
            Some(MutationToken::try_from(extras)?)
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(DecrementResponse {
            cas: packet.cas.unwrap_or_default(),
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

impl TryFromClientResponse for LookupInResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let subdoc_info = resp.response_context().subdoc_info.ok_or_else(|| Error::protocol_error("Missing subdoc info in response context"))?;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocInvalidCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::InvalidCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocInvalidXattrOrder {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        }  else if status == Status::SubDocXattrInvalidKeyCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        }

        let mut doc_is_deleted = false;

        if status == Status::SubDocSuccessDeleted || status == Status::SubDocMultiPathFailureDeleted {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(subdoc_info.op_count as usize);
        let mut op_index = 0;

        let value = resp.packet().value.as_ref().ok_or_else(|| Error::protocol_error("Missing value"))?;
        let mut cursor = Cursor::new(value);

        while cursor.position() < cursor.get_ref().len() as u64 {
            if cursor.remaining() < 6 {
                return Err(Error::protocol_error("bad value length"));
            }

            let res_status = cursor
                .read_u16::<BigEndian>()
                .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;
            let res_status = Status::from(res_status);
            let res_value_len = cursor
                .read_u32::<BigEndian>()
                .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;

            if cursor.remaining() < res_value_len as usize {
                return Err(Error::protocol_error("bad value length"));
            }

            let value = if res_value_len > 0 {
                let mut tmp_val = vec![0; res_value_len as usize];
                cursor.read_exact(&mut tmp_val)
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;
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
                _ => Some(SubdocErrorKind::UnknownStatus {status: res_status}),
            };

            let err = err_kind.map(|kind| SubdocError { kind, op_index: Some(op_index) });

            results.push(SubDocResult { value, err });
            op_index += 1;
        }

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(LookupInResponse {
            cas: resp.packet().cas.unwrap_or_default(),
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

impl TryFromClientResponse for MutateInResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        let subdoc_info = resp.response_context().subdoc_info.ok_or_else(|| Error::protocol_error("Missing subdoc info in response context"))?;

        if status == Status::KeyNotFound {
            return Err(ServerError::new(
                ServerErrorKind::KeyNotFound,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::KeyExists && resp.response_context().cas.is_some() {
            return Err(ServerError::new(
                ServerErrorKind::CasMismatch,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::Locked {
            return Err(ServerError::new(
                ServerErrorKind::Locked,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::TooBig {
            return Err(ServerError::new(
                ServerErrorKind::TooBig,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocInvalidCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::InvalidCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocInvalidXattrOrder {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::InvalidXattrOrder, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        }  else if status == Status::SubDocXattrInvalidKeyCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrInvalidKeyCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocXattrInvalidFlagCombo {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrInvalidFlagCombo, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocXattrUnknownMacro {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrUnknownMacro, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocXattrUnknownVattrMacro {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrUnknownVattrMacro, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocXattrCannotModifyVAttr {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::XattrCannotModifyVAttr, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocCanOnlyReviveDeletedDocuments {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::CanOnlyReviveDeletedDocuments, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocDeletedDocumentCantHaveValue {
            return Err(ServerError::new(
                ServerErrorKind::Subdoc{ error: SubdocError::new(SubdocErrorKind::DeletedDocumentCantHaveValue, None) },
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::NotStored {
            if subdoc_info.flags.is_some_and(|flags| flags == SubdocDocFlag::AddDoc){
                return Err(ServerError::new(
                    ServerErrorKind::KeyExists,
                    resp.packet(),
                    resp.local_addr(),
                    resp.peer_addr()
                ).into());
            }
            return Err(ServerError::new(
                ServerErrorKind::NotStored,
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ).into());
        } else if status == Status::SubDocMultiPathFailure {
            if let Some(value) = &resp.packet().value {
                if value.len() != 3 {
                    return Err(Error::protocol_error("bad value length"));
                }
                let mut cursor = Cursor::new(value);

                let op_index = cursor
                    .read_u8()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;
                let res_status = cursor
                    .read_u16::<BigEndian>()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;
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
                    _ => SubdocErrorKind::UnknownStatus {status: res_status},
                };

                return Err(ServerError::new(
                    ServerErrorKind::Subdoc{ error: SubdocError::new(err_kind, Some(op_index)) },
                    resp.packet(),
                    resp.local_addr(),
                    resp.peer_addr()
                ).into());
            }
        }

        let mut doc_is_deleted = false;
        if status == Status::SubDocSuccessDeleted {
            doc_is_deleted = true;
            // still considered a success
        } else if status != Status::Success && status != Status::SubDocMultiPathFailure {
            return Err(OpsCrud::decode_common_error(
                resp.packet(),
                resp.local_addr(),
                resp.peer_addr(),
            ));
        }

        let mut results: Vec<SubDocResult> = Vec::with_capacity(subdoc_info.op_count as usize);

        if let Some(value) = &resp.packet().value {
            let mut cursor = Cursor::new(value);

            while cursor.position() < cursor.get_ref().len() as u64 {
                if cursor.remaining() < 3 {
                    return Err(Error::protocol_error("bad value length"));
                }

                let op_index = cursor
                    .read_u8()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;

                if op_index > results.len() as u8 {
                    for _ in results.len() as u8..op_index {
                        results.push(SubDocResult {
                            err: None,
                            value: None,
                        });
                    }
                }

                let op_status = cursor
                    .read_u16::<BigEndian>()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;
                let op_status = Status::from(op_status);

                if op_status == Status::Success {
                    let val_length = cursor
                        .read_u32::<BigEndian>()
                        .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?;

                    let mut value = vec![0; val_length as usize];
                    cursor.read_exact(&mut value)?;

                    results.push(SubDocResult {
                        err: None,
                        value: Some(value),
                    });
                } else {
                    return Err(Error::protocol_error("subdoc mutatein op illegally provided an error"));
                }
            }
        }

        if results.len() < subdoc_info.op_count as usize {
            for _ in results.len()..subdoc_info.op_count as usize {
                results.push(SubDocResult {
                    err: None,
                    value: None,
                });
            }
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            if extras.len() != 16 {
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                    .into());
            }
            let mut extras = Cursor::new(extras);

            Some(MutationToken {
                vbuuid: extras
                    .read_u64::<BigEndian>()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?,
                seqno: extras
                    .read_u64::<BigEndian>()
                    .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?,
            })
        } else {
            None
        };

        let server_duration = if let Some(f) = &packet.framing_extras {
            decode_res_ext_frames(f)?
        } else {
            None
        };

        Ok(MutateInResponse {
            cas: resp.packet().cas.unwrap_or_default(),
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

impl TryFromClientResponse for GetCollectionIdResponse {
    fn try_from(resp: ClientResponse) -> Result<Self, Error> {
        let packet = resp.packet();
        let status = packet.status;

        if status == Status::ScopeUnknown {
            return Err(ErrorKind::Resource(ResourceError {
                cause: OpsCore::decode_error_context(
                    packet,
                    ServerErrorKind::UnknownScopeName,
                    resp.local_addr(),
                    resp.peer_addr(),
                ),
                scope_name: "".to_string(),
                collection_name: None,
            })
            .into());
        } else if status == Status::CollectionUnknown {
            return Err(ErrorKind::Resource(ResourceError {
                cause: OpsCore::decode_error_context(
                    packet,
                    ServerErrorKind::UnknownCollectionName,
                    resp.local_addr(),
                    resp.peer_addr(),
                ),
                scope_name: "".to_string(),
                collection_name: Some("".to_string()),
            })
            .into());
        } else if status != Status::Success {
            return Err(
                OpsCore::decode_error(resp.packet(), resp.local_addr(), resp.peer_addr()).into(),
            );
        }

        let extras = if let Some(extras) = &packet.extras {
            if extras.len() != 12 {
                return Err(Error::protocol_error("invalid extras length"));
            }
            extras
        } else {
            return Err(Error::protocol_error("no extras in response"));
        };

        let mut extras = Cursor::new(extras);
        let manifest_rev = extras.read_u64::<BigEndian>().map_err(|e| {
            Error::protocol_error_with_source(
                "failed to read manifest rev from extras",
                Box::new(e),
            )
        })?;

        let collection_id = extras.read_u32::<BigEndian>().map_err(|e| {
            Error::protocol_error_with_source(
                "failed to read collection id from extras",
                Box::new(e),
            )
        })?;

        Ok(GetCollectionIdResponse {
            manifest_rev,
            collection_id,
        })
    }
}
