use std::time::Duration;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::durability_level::DurabilityLevel;
use crate::memdx::hello_feature::HelloFeature;
use crate::memdx::subdoc::{LookupInOp, MutateInOp, SubdocDocFlag};

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
pub struct GetCollectionIdRequest {
    pub scope_name: String,
    pub collection_name: String,
}

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
pub struct SetRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub flags: u32,
    pub value: &'a [u8],
    pub datatype: u8,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetMetaRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DeleteRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndLockRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub lock_time: u32,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GetAndTouchRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub expiry: u32,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnlockRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub cas: u64,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TouchRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub expiry: u32,
    pub on_behalf_of: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AddRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub flags: u32,
    pub value: &'a [u8],
    pub datatype: u8,
    pub expiry: Option<u32>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReplaceRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub flags: u32,
    pub value: &'a [u8],
    pub datatype: u8,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AppendRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub value: &'a [u8],
    pub datatype: u8,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PrependRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub value: &'a [u8],
    pub datatype: u8,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IncrementRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub expiry: Option<u32>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DecrementRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub initial: Option<u64>,
    pub delta: Option<u64>,
    pub expiry: Option<u32>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}

pub struct LookupInRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub flags: Option<SubdocDocFlag>,
    pub ops: &'a [LookupInOp<'a>],
    pub on_behalf_of: Option<String>,
}

pub struct MutateInRequest<'a> {
    pub collection_id: u32,
    pub key: &'a [u8],
    pub vbucket_id: u16,
    pub flags: Option<SubdocDocFlag>,
    pub ops: &'a [MutateInOp<'a>],
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub on_behalf_of: Option<String>,
    pub durability_level: Option<DurabilityLevel>,
    pub durability_level_timeout: Option<Duration>,
}
