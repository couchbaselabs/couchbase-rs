use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::error::Error;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::ops_core::decode_error;
use crate::memdx::packet::ResponsePacket;
use crate::memdx::status::Status;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

pub trait TryFromResponsePacket: Sized {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error>;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloResponse {
    pub enabled_features: Vec<HelloFeature>,
}

impl TryFromResponsePacket for HelloResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        let mut features: Vec<HelloFeature> = Vec::new();
        if let Some(value) = packet.value() {
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

impl TryFromResponsePacket for GetErrorMapResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        // TODO: Clone?
        let value = packet.value().clone().unwrap_or_default();
        let response = GetErrorMapResponse { error_map: value };

        Ok(response)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketResponse {}

impl TryFromResponsePacket for SelectBucketResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        Ok(SelectBucketResponse {})
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl TryFromResponsePacket for SASLAuthResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        Ok(SASLAuthResponse {
            needs_more_steps: false,
            // TODO: clone?
            payload: packet.value().clone().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLStepResponse {
    pub needs_more_steps: bool,
    pub payload: Vec<u8>,
}

impl TryFromResponsePacket for SASLStepResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        Ok(SASLStepResponse {
            needs_more_steps: false,
            // TODO: clone?
            payload: packet.value().clone().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsResponse {
    pub available_mechs: Vec<AuthMechanism>,
}

impl TryFromResponsePacket for SASLListMechsResponse {
    fn try_from(packet: ResponsePacket) -> Result<Self, Error> {
        let status = packet.status();
        if status != Status::Success {
            return Err(decode_error(packet));
        }

        // TODO: Clone?
        let value = packet.value().clone().unwrap_or_default();
        let mechs_list_string = match String::from_utf8(value) {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::Protocol(e.to_string()));
            }
        };
        let mechs_list_split = mechs_list_string.split(" ");
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
pub struct BootstrapResult {
    pub hello: Option<HelloResponse>,
    pub error_map: Option<GetErrorMapResponse>,
}
