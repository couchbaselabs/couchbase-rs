use std::sync::Arc;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;

#[derive(Clone, Debug)]
pub struct GetOptions {
    pub key: Vec<u8>,
    pub scope_name: String,
    pub collection_name: String,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

#[derive(Clone, Debug)]
pub struct UpsertOptions {
    pub key: Vec<u8>,
    pub scope_name: String,
    pub collection_name: String,
    pub value: Vec<u8>,
    pub flags: u32,
    pub datatype: DataTypeFlag,
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}
