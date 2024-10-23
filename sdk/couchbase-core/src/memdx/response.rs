use std::io::Cursor;
use std::time::Duration;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::{Error, ErrorKind, ResourceError, ServerError, ServerErrorKind};
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::{decode_res_ext_frames, OpsCrud};
use crate::memdx::status::Status;
use byteorder::{BigEndian, ReadBytesExt};

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
                return Err(ErrorKind::Protocol {
                    msg: "invalid hello features length".into(),
                }
                .into());
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
                return Err(ErrorKind::Protocol { msg: e.to_string() }.into());
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
                return Err(ErrorKind::Protocol {
                    msg: "Failed to identify memd hostname for $HOST replacement".to_string(),
                }
                .into());
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

        Ok(SetResponse {
            cas: packet.cas.unwrap_or_default(),
            mutation_token,
            server_duration,
        })
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

        let flags = if let Some(extras) = &packet.extras {
            if extras.len() != 4 {
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                .into());
            }

            let mut extras = Cursor::new(extras);
            extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?
        } else {
            return Err(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            }
            .into());
        };

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
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                .into());
            }

            let mut extras = Cursor::new(extras);
            let deleted = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?;
            let flags = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?;
            let expiry = extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?;
            let seq_no = extras.read_u64::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?;
            let datatype = extras.read_u8().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
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
            Err(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            }
            .into())
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

        let flags = if let Some(extras) = &packet.extras {
            if extras.len() != 4 {
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                .into());
            }

            let mut extras = Cursor::new(extras);
            extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?
        } else {
            return Err(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            }
            .into());
        };

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

        let flags = if let Some(extras) = &packet.extras {
            if extras.len() != 4 {
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                .into());
            }

            let mut extras = Cursor::new(extras);
            extras.read_u32::<BigEndian>().map_err(|e| {
                Error::from(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                })
            })?
        } else {
            return Err(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            }
            .into());
        };

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
                return Err(ErrorKind::Protocol {
                    msg: "Bad extras length".to_string(),
                }
                .into());
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

        if status == Status::KeyExists {
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

        if status == Status::KeyExists {
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
                return Err(ErrorKind::Protocol {
                    msg: "Bad value length".to_string(),
                }
                .into());
            }
            let mut val = Cursor::new(val);

            val.read_u64::<BigEndian>()
                .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?
        } else {
            0
        };

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
                return Err(ErrorKind::Protocol {
                    msg: "Bad value length".to_string(),
                }
                .into());
            }
            let mut val = Cursor::new(val);

            val.read_u64::<BigEndian>()
                .map_err(|e| Error::from(ErrorKind::Protocol { msg: e.to_string() }))?
        } else {
            0
        };

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

        Ok(DecrementResponse {
            cas: packet.cas.unwrap_or_default(),
            value,
            mutation_token,
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
                return Err(ErrorKind::Protocol {
                    msg: "Invalid extras length".to_string(),
                }
                .into());
            }
            extras
        } else {
            return Err(ErrorKind::Protocol {
                msg: "Invalid extras length".to_string(),
            }
            .into());
        };

        let mut extras = Cursor::new(extras);
        let manifest_rev = extras.read_u64::<BigEndian>().map_err(|e| {
            Error::from(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            })
        })?;

        let collection_id = extras.read_u32::<BigEndian>().map_err(|e| {
            Error::from(ErrorKind::Protocol {
                msg: "Bad extras length".to_string(),
            })
        })?;

        Ok(GetCollectionIdResponse {
            manifest_rev,
            collection_id,
        })
    }
}
