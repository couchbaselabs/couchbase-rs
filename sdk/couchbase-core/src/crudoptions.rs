use crate::memdx::durability_level::DurabilityLevel;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GetOptions {
    pub key: Vec<u8>,
    pub scope_name: String,
    pub collection_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpsertOptions {
    pub key: Vec<u8>,
    pub scope_name: String,
    pub collection_name: String,
    pub value: Vec<u8>,
    pub flags: u32,
    // pub datatype:
    pub expiry: Option<u32>,
    pub preserve_expiry: Option<bool>,
    pub cas: Option<u64>,
    pub durability_level: Option<DurabilityLevel>,
}
