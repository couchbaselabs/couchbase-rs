use std::io::Cursor;
use std::time::Duration;

use byteorder::{BigEndian, ReadBytesExt};

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client_response::ClientResponse;
use crate::memdx::error::Error;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::ops_crud::{decode_res_ext_frames, OpsCrud};
use crate::memdx::status::Status;

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
            return Err(OpsCore::decode_error(packet));
        }

        let mut features: Vec<HelloFeature> = Vec::new();
        if let Some(value) = &packet.value {
            if value.len() % 2 != 0 {
                return Err(Error::Protocol("invalid hello features length".into()));
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
            return Err(OpsCore::decode_error(packet));
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
                return Err(Error::UnknownBucketName);
            }
            return Err(OpsCore::decode_error(packet));
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
            return Err(OpsCore::decode_error(packet));
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
            return Err(OpsCore::decode_error(packet));
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
                return Err(Error::ConfigNotSet);
            }
            return Err(OpsCore::decode_error(packet));
        }

        // TODO: Clone?
        let value = packet.value.clone().unwrap_or_default();
        let mechs_list_string = match String::from_utf8(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::Protocol(e.to_string()));
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
            return Err(OpsCore::decode_error(packet));
        }

        let host = match resp.local_addr() {
            Some(addr) => addr.ip().to_string(),
            None => {
                return Err(Error::Generic(
                    "Failed to identify memd hostname for $HOST replacement".to_string(),
                ))
            }
        };

        // TODO: Clone, maybe also inefficient?
        let value = match std::str::from_utf8(packet.value.clone().unwrap_or_default().as_slice()) {
            Ok(v) => v.to_string(),
            Err(e) => "".to_string(),
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
            return Err(Error::TooBig);
        } else if status == Status::Locked {
            return Err(Error::Locked);
        } else if status == Status::KeyExists {
            return Err(Error::KeyExists);
        } else if status != Status::Success {
            return Err(Error::Unknown(
                OpsCrud::decode_common_error(resp.packet()).to_string(),
            ));
        }

        let mutation_token = if let Some(extras) = &packet.extras {
            if extras.len() != 16 {
                return Err(Error::Protocol("Bad extras length".to_string()));
            }
            let mut extras = Cursor::new(extras);

            Some(MutationToken {
                vbuuid: extras
                    .read_u64::<BigEndian>()
                    .map_err(|e| Error::Unknown(e.to_string()))?,
                seqno: extras
                    .read_u64::<BigEndian>()
                    .map_err(|e| Error::Unknown(e.to_string()))?,
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
            return Err(Error::KeyNotFound);
        } else if status != Status::Success {
            return Err(Error::Unknown(
                OpsCrud::decode_common_error(resp.packet()).to_string(),
            ));
        }

        let flags = if let Some(extras) = &packet.extras {
            if extras.len() != 4 {
                return Err(Error::Protocol("Bad extras length".to_string()));
            }

            let mut extras = Cursor::new(extras);
            extras
                .read_u32::<BigEndian>()
                .map_err(|e| Error::Unknown(e.to_string()))?
        } else {
            return Err(Error::Protocol("Bad extras length".to_string()));
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
