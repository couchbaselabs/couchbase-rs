use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::subdoc::{MutateInOp, LookupInOp, SubdocDocFlag};
use crate::memdx::durability_level::DurabilityLevel;
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct GetOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct GetMetaOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct UpsertOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    #[builder(default)]
    pub flags: u32,
    #[builder(default)]
    pub datatype: DataTypeFlag,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub preserve_expiry: Option<bool>,
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct DeleteOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct GetAndLockOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub lock_time: u32,
    #[builder(default, setter(into))]
    pub collection_id: Option<u32>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct GetAndTouchOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub expiry: u32,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct UnlockOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub cas: u64,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct TouchOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub expiry: u32,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct AddOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    #[builder(default)]
    pub flags: u32,
    #[builder(default)]
    pub datatype: DataTypeFlag,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct ReplaceOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    #[builder(default)]
    pub flags: u32,
    #[builder(default)]
    pub datatype: DataTypeFlag,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub preserve_expiry: Option<bool>,
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct AppendOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct PrependOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct IncrementOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default, setter(into))]
    pub initial: Option<u64>,
    #[builder(default, setter(into))]
    pub delta: Option<u64>,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct DecrementOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    #[builder(default, setter(into))]
    pub initial: Option<u64>,
    #[builder(default, setter(into))]
    pub delta: Option<u64>,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct LookupInOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub ops: &'a [LookupInOp<'a>],
    #[builder(default, setter(into))]
    pub flags: Option<SubdocDocFlag>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct MutateInOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub ops: &'a [MutateInOp<'a>],
    #[builder(default, setter(into))]
    pub flags: Option<SubdocDocFlag>,
    #[builder(default, setter(into))]
    pub expiry: Option<u32>,
    #[builder(default, setter(into))]
    pub preserve_expiry: Option<bool>,
    #[builder(default, setter(into))]
    pub cas: Option<u64>,
    #[builder(default, setter(into))]
    pub durability_level: Option<DurabilityLevel>,
    #[builder(default=DEFAULT_RETRY_STRATEGY.clone())]
    pub retry_strategy: Arc<dyn RetryStrategy>,
}
