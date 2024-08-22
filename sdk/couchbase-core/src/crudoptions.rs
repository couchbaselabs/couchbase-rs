use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;

#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct GetOptions<'a> {
    pub key: &'a [u8],
    pub scope_name: &'a str,
    pub collection_name: &'a str,
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
    pub retry_strategy: Arc<dyn RetryStrategy>,
}
