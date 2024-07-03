use std::time::Duration;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::durability_level::DurabilityLevel;
use crate::memdx::hello_feature::HelloFeature;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HelloRequest {
    pub client_name: Vec<u8>,
    pub requested_features: Vec<HelloFeature>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetErrorMapRequest {
    pub version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SelectBucketRequest {
    pub bucket_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetClusterConfigRequest {}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthRequest {
    pub auth_mechanism: AuthMechanism,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLStepRequest {
    pub auth_mechanism: AuthMechanism,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLListMechsRequest {}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SetRequest {
    pub collection_id: u32,
    pub key: Vec<u8>,
    pub vbucket_id: u16,
    pub flags: u32,
    pub value: Vec<u8>,
    pub datatype: u8,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetRequest {
    pub collection_id: u32,
    pub key: Vec<u8>,
    pub vbucket_id: u16,
    pub on_behalf_of: Option<String>,
}
