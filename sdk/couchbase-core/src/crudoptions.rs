use std::sync::Arc;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;

#[derive(Clone, Debug)]
pub struct GetOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug)]
pub struct UpsertOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
    pub value: &'a [u8],
    pub flags: u32,
    pub datatype: DataTypeFlag,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}
