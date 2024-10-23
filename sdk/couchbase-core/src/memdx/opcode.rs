use std::fmt::{Display, Formatter};

use crate::memdx::error::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum OpCode {
    Get,
    Set,
    Add,
    Replace,
    Delete,
    Increment,
    Decrement,
    Touch,
    GAT,
    Append,
    Prepend,
    Hello,
    GetClusterConfig,
    GetCollectionId,
    GetErrorMap,
    SelectBucket,
    GetLocked,
    UnlockKey,
    GetMeta,
    SASLAuth,
    SASLListMechs,
    SASLStep,
    Unknown(u8),
}

impl From<OpCode> for u8 {
    fn from(value: OpCode) -> Self {
        match value {
            OpCode::Get => 0x00,
            OpCode::Set => 0x01,
            OpCode::Add => 0x02,
            OpCode::Replace => 0x03,
            OpCode::Delete => 0x04,
            OpCode::Increment => 0x05,
            OpCode::Decrement => 0x06,
            OpCode::Append => 0x0e,
            OpCode::Prepend => 0x0f,
            OpCode::Touch => 0x1c,
            OpCode::GAT => 0x1d,
            OpCode::Hello => 0x1f,
            OpCode::SASLListMechs => 0x20,
            OpCode::SASLAuth => 0x21,
            OpCode::SASLStep => 0x22,
            OpCode::SelectBucket => 0x89,
            OpCode::GetLocked => 0x94,
            OpCode::UnlockKey => 0x95,
            OpCode::GetMeta => 0xa0,
            OpCode::GetClusterConfig => 0xb5,
            OpCode::GetCollectionId => 0xbb,
            OpCode::GetErrorMap => 0xfe,
            OpCode::Unknown(code) => code,
        }
    }
}

impl TryFrom<u8> for OpCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let code = match value {
            0x00 => OpCode::Get,
            0x01 => OpCode::Set,
            0x02 => OpCode::Add,
            0x03 => OpCode::Replace,
            0x04 => OpCode::Delete,
            0x05 => OpCode::Increment,
            0x06 => OpCode::Decrement,
            0x0e => OpCode::Append,
            0x0f => OpCode::Prepend,
            0x1c => OpCode::Touch,
            0x1d => OpCode::GAT,
            0x1f => OpCode::Hello,
            0x20 => OpCode::SASLListMechs,
            0x21 => OpCode::SASLAuth,
            0x22 => OpCode::SASLStep,
            0x89 => OpCode::SelectBucket,
            0x94 => OpCode::GetLocked,
            0x95 => OpCode::UnlockKey,
            0xb5 => OpCode::GetClusterConfig,
            0xbb => OpCode::GetCollectionId,
            0xfe => OpCode::GetErrorMap,
            _ => OpCode::Unknown(value),
        };

        Ok(code)
    }
}

impl Display for OpCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let txt = match self {
            OpCode::Get => "Get",
            OpCode::Set => "Set",
            OpCode::Add => "Add",
            OpCode::Replace => "Replace",
            OpCode::Delete => "Delete",
            OpCode::Increment => "Increment",
            OpCode::Decrement => "Decrement",
            OpCode::Append => "Append",
            OpCode::Prepend => "Prepend",
            OpCode::Touch => "Touch",
            OpCode::GAT => "GAT",
            OpCode::GetMeta => "Get meta",
            OpCode::Hello => "Hello",
            OpCode::GetClusterConfig => "Get cluster config",
            OpCode::GetCollectionId => "Get collection id",
            OpCode::GetErrorMap => "Get error map",
            OpCode::SelectBucket => "Select bucket",
            OpCode::GetLocked => "Get locked",
            OpCode::UnlockKey => "Unlock key",
            OpCode::SASLAuth => "SASL auth",
            OpCode::SASLListMechs => "SASL list mechanisms",
            OpCode::SASLStep => "SASL step",
            OpCode::Unknown(code) => {
                return write!(f, "x{:02x}", code);
            }
        };
        write!(f, "{}", txt)
    }
}
